#!/usr/bin/env ruby
# Source-bound same-host diagnosis; never replaces checkpoint acceptance.
require_relative 'diagnostic_support'
require 'etc'

module CheckpointPair
  extend DiagnosticSupport
  CELLS = { 'A' => ['checkpoint_local_256x1mib_1cf', 256, 1024, 1], 'B' => ['checkpoint_local_512x256kib_16cf', 512, 256, 16], 'C' => ['checkpoint_local_1024x64kib_1cf', 1024, 64, 1] }.freeze
  BINARY = 'tier4_system_checkpoint_write_amplification'
  BENCHMARK_ENV = %w[STRESS_PROFILE STRESS_SAMPLES STRESS_WARMUP_SAMPLES STRESS_COOLDOWN_SAMPLES STRESS_CONFIRM_REGRESSIONS STRESS_TIMEOUT_SECS STRESS_NO_PROGRESS_TIMEOUT_SECS STRESS_GIT_SHA MIDGE_CHECKPOINT_REPEAT STRESS_RUN_ID STRESS_OUTPUT_DIR MIDGE_STRESS_ARTIFACT_DIR TMPDIR PATH GITHUB_SHA].freeze

  def self.plan
    (1..3).flat_map { |block| (block.odd? ? %w[baseline candidate candidate baseline] : %w[candidate baseline baseline candidate]).map { |role| [block, role] } }
  end

  def self.verify_source(source, sha, manifest)
    check(checked(%w[git rev-parse HEAD], cwd: source) == sha, 'actual checkout differs from declared immutable source')
    check(checked(%w[git status --porcelain], cwd: source).empty?, 'source checkout is dirty')
    check(manifest['sha'] == sha && manifest['tree'] == checked(['git', 'rev-parse', 'HEAD^{tree}'], cwd: source) && manifest['cargo_lock_sha256'] == digest(File.join(source, 'Cargo.lock')) && manifest['profile'] == 'bench/release' && manifest['binary'] == BINARY, 'build/source/lock identity mismatch')
  end

  def self.download_build(repository, run, sha, source, root)
    Dir.mkdir(root)
    api = "repos/#{repository}/actions"
    run_data = JSON.parse(checked(['gh', 'api', "#{api}/runs/#{run}/attempts/1"]))
    save(File.join(root, 'run.json'), run_data)
    check(run_data['id'] == run && run_data['head_sha'] == sha && run_data['run_attempt'] == 1 && run_data['status'] == 'completed' && run_data['conclusion'] == 'success', 'origin run is not a completed successful source match')
    pages = JSON.parse(checked(['gh', 'api', '--paginate', '--slurp', "#{api}/runs/#{run}/attempts/1/jobs?per_page=100"]))
    save(File.join(root, 'jobs.json'), pages)
    builds = pages.flat_map { |page| page.fetch('jobs') }.select { |job| job['name'] == 'checkpoint-build' }
    check(builds.size == 1 && builds[0]['conclusion'] == 'success', 'missing successful originating build job')
    pages = JSON.parse(checked(['gh', 'api', '--paginate', '--slurp', "#{api}/runs/#{run}/artifacts?per_page=100"]))
    save(File.join(root, 'artifacts.json'), pages)
    matches = pages.flat_map { |page| page.fetch('artifacts') }.select { |artifact| artifact['name'] == "checkpoint-build-#{sha}-#{run}-a1" }
    check(matches.size == 1 && !matches[0]['expired'], 'missing exact original build artifact')
    artifact = matches.first
    archive = File.join(root, 'provider.zip')
    check(system('gh', 'api', "#{api}/artifacts/#{artifact['id']}/zip", out: archive), 'artifact download failed')
    check(artifact['digest'] == "sha256:#{digest(archive)}", 'original provider archive digest mismatch')
    entries = checked(['unzip', '-Z1', archive]).lines.map(&:chomp)
    check(entries.all? { |name| !name.start_with?('/') && !name.split('/').include?('..') && !name.include?('\\') }, 'invalid archive member path')
    files = File.join(root, 'files')
    checked(['unzip', '-q', archive, '-d', files])
    manifests = Dir.glob(File.join(files, '**', 'build-manifest.json'))
    check(manifests.size == 1, 'ambiguous build manifest')
    manifest = json(manifests.first)
    check(manifest['run_id'] == run.to_s && manifest['run_attempt'] == '1', 'build manifest has another run identity')
    verify_source(source, sha, manifest)
    binary = File.join(File.dirname(manifests.first), BINARY)
    check(digest(binary) == manifest['executable_sha256'], 'actual release executable differs from build receipt')
    File.chmod(0755, binary)
    { 'sha' => sha, 'source' => source, 'binary' => binary, 'manifest' => manifest, 'run_id' => run, 'artifact_id' => artifact['id'], 'archive_sha256' => digest(archive) }
  end

  def self.native_result(root, sha, cell = 'A')
    workload, cycles, rows, families = CELLS.fetch(cell)
    warmup = (cycles / 10.0).ceil
    paths = Dir.glob(File.join(root, 'midge', '*', 'workload-status.json'))
    check(paths.size == 1, 'missing unique actual engine receipt')
    directory = File.dirname(paths.first)
    status = json(paths.first)
    check(status['git_commit'] == sha && status['status'] == 'passed' && status['phase'] == 'complete' && status['terminal_error'].nil? && status['verified'] && status['reopened_verified'] && status['cell'] == cell && status['benchmark_workload'] == workload && status['completed_cycles'] == cycles && status['total_cycles'] == cycles && status['warmup_cycles'] == warmup && status['rows_per_cycle'] == rows && status['families'] == families && status['acknowledged_rows'] == cycles * rows, 'source/shape/ACK/reopen proof failed')
    window = json(File.join(directory, 'accounting-window.json'))
    latencies = json(File.join(directory, 'ingestion-observations.json')).fetch('flush_latencies_ns').sort
    check(latencies.size == status['measured_cycles'] && latencies.size == cycles - warmup, 'public flush distribution does not cover fixed measured cycles')
    check(window.dig('gate', 'valid'), 'native accounting gate is invalid')
    { 'measured_elapsed_ns' => window['measured_elapsed_ns'], 'flush_p95_ns' => latencies[(latencies.size * 0.95).ceil - 1], 'completed_compactions' => window['completed_compactions'], 'runtime_before' => json(File.join(directory, 'runtime-before.json')), 'runtime_after' => json(File.join(directory, 'runtime-after.json')), 'checkpoint_gate' => window['gate'], 'verified' => true }
  end

  def self.cpu_command(command, root, environment)
    child = ['runuser', '-u', Etc.getpwuid(Process.uid).name, '--', 'env']
    child.concat(BENCHMARK_ENV.select { |key| environment.key?(key) }.map { |key| "#{key}=#{environment[key]}" })
    ['sudo', '-n', 'perf', 'record', '-F', '99', '-g', '--call-graph', 'dwarf', '-o', File.join(root, 'perf.data'), '--', *child, *command]
  end

  def self.trial(build, block, root, instrument = nil, cell = 'A')
    Dir.mkdir(root)
    %w[native midge].each { |name| Dir.mkdir(File.join(root, name)) }
    memory_root = File.join('/dev/shm', "midge-pair-#{ENV.fetch('GITHUB_RUN_ID')}-#{File.basename(root)}")
    Dir.mkdir(memory_root)
    %w[native midge].each { |name| Dir.mkdir(File.join(memory_root, name)) }
    verify_source(build['source'], build['sha'], build['manifest'])
    check(digest(build['binary']) == build['manifest']['executable_sha256'], 'executable changed between trials')
    environment = ENV.to_h.merge('STRESS_PROFILE' => 'smoke', 'STRESS_SAMPLES' => '1', 'STRESS_WARMUP_SAMPLES' => '0', 'STRESS_COOLDOWN_SAMPLES' => '0', 'STRESS_CONFIRM_REGRESSIONS' => '0', 'STRESS_TIMEOUT_SECS' => '900', 'STRESS_NO_PROGRESS_TIMEOUT_SECS' => '60', 'STRESS_GIT_SHA' => build['sha'], 'MIDGE_CHECKPOINT_REPEAT' => block.to_s, 'STRESS_RUN_ID' => File.basename(root), 'STRESS_OUTPUT_DIR' => File.join(memory_root, 'native'), 'MIDGE_STRESS_ARTIFACT_DIR' => File.join(memory_root, 'midge'))
    command = ['/usr/bin/time', '-v', '-o', File.join(root, 'process-time.txt'), build['binary'], '--workload', CELLS.fetch(cell)[0], '--bench']
    command = ['strace', '-f', '-c', '-e', 'trace=fsync,fdatasync', '-o', File.join(root, 'sync-summary.txt'), *command] if instrument == 'sync'
    command = cpu_command(command, root, environment) if instrument == 'cpu'
    checked(['sync'])
    result = { 'sha' => build['sha'], 'binary_sha256' => digest(build['binary']), 'instrument' => instrument, 'valid' => false, 'directory' => root }
    started = clock
    log = File.join(root, 'workload.log')
    outcome = run_process(command, out: log, err: [:child, :out], timeout: 900, env: environment, cwd: build['source'], privileged: instrument == 'cpu')
    result['returncode'] = outcome['exit_code']
    result['error'] = outcome['error'] if outcome['error']
    result['process_wall_seconds'] = clock - started
    data = File.join(root, 'perf.data')
    checked(['sudo', '-n', 'chown', "#{Process.uid}:#{Process.gid}", data]) if instrument == 'cpu' && File.exist?(data)
    %w[native midge].each { |name| FileUtils.cp_r(File.join(memory_root, name, '.'), File.join(root, name)) }
    FileUtils.remove_entry(memory_root)
    memory_root = nil
    begin
      result.merge!(native_result(root, build['sha'], cell))
      check(result['returncode'] == 0 && !result['error'], 'benchmark/instrument process failed')
      result['valid'] = true
    rescue StandardError => error
      result['error'] = error.message
    end
    if instrument == 'cpu' && result['valid']
      report = run_process(['perf', 'report', '--stdio', '--no-children', '-i', data], out: File.join(root, 'perf-report.txt'), err: [:child, :out], timeout: 60)
      result['profile_report_returncode'] = report['exit_code']
      result.merge!('valid' => false, 'error' => 'CPU profile analysis failed; raw data retained') if report['exit_code'] != 0 || report['error']
    end
    save(File.join(root, 'trial.json'), result)
    result
  ensure
    # Preserve any partial receipts before releasing this trial's tmpfs copy.
    if memory_root && File.directory?(memory_root)
      %w[native midge].each do |name|
        from = File.join(memory_root, name, '.')
        FileUtils.cp_r(from, File.join(root, name)) if File.directory?(from)
      end
      FileUtils.remove_entry(memory_root)
    end
  end

  def self.main
    options = { 'cells' => ['A'], 'host-repeat' => 1 }
    parser = OptionParser.new do |cli|
      %w[repository baseline-sha candidate-sha baseline-source candidate-source output].each { |key| cli.on("--#{key} VALUE") { |value| options[key] = value } }
      %w[baseline-run candidate-run host-repeat].each { |key| cli.on("--#{key} NUMBER", Integer) { |value| options[key] = value } }
      cli.on('--cells CELLS', Array) { |value| options['cells'] = value }
    end
    parser.parse!
    check(ARGV.empty? && %w[repository baseline-run candidate-run baseline-sha candidate-sha baseline-source candidate-source output].all? { |key| options.key?(key) }, parser.to_s)
    cells = options['cells']
    host = options['host-repeat']
    check(!cells.empty? && cells.uniq == cells && cells.all? { |cell| CELLS.key?(cell) } && (1..3).cover?(host), 'invalid cell or host selection')
    root = File.expand_path(options['output'])
    Dir.mkdir(root)
    report = { 'schema_version' => 'midge-checkpoint-pair-diagnostic.v1', 'accepted' => false, 'workflow_sha' => ENV['GITHUB_SHA'], 'cells' => cells, 'host_repeat' => host, 'original_workload_limits_unchanged' => true, 'complete' => false, 'planned_uninstrumented_trials' => 12 * cells.size, 'trials' => [], 'profiles' => [] }
    save(File.join(root, 'report.json'), report)
    begin
      builds = %w[baseline candidate].to_h { |role| [role, download_build(options['repository'], options["#{role}-run"], options["#{role}-sha"], File.expand_path(options["#{role}-source"]), File.join(root, role))] }
      toolchain = builds['baseline']['manifest']['toolchain']
      check(toolchain == builds['candidate']['manifest']['toolchain'], 'sources were built with different toolchains')
      checked(['rustup', 'toolchain', 'install', toolchain, '--profile', 'minimal'])
      checked(['rustup', 'default', toolchain])
      ordered = cells.rotate((host - 1) % cells.size)
      ordered.each do |cell|
        plan.each_with_index do |(block, role), offset|
          index = offset + 1
          item = trial(builds[role], block, File.join(root, format('timing-%s-%02d-%s', cell, index, role)), nil, cell)
          item.merge!('cell' => cell, 'block' => block, 'role' => role, 'index' => index)
          report['trials'] << item
          save(File.join(root, 'report.json'), report)
          puts JSON.generate(item.select { |key, _| %w[cell index role valid measured_elapsed_ns flush_p95_ns completed_compactions error].include?(key) })
          $stdout.flush
        end
      end
      report['complete'] = report['trials'].size == report['planned_uninstrumented_trials'] && report['trials'].all? { |item| item['valid'] }
      save(File.join(root, 'report.json'), report)
      ordered.product(%w[sync cpu], %w[baseline candidate]).each do |cell, instrument, role|
        begin
          item = trial(builds[role], 1, File.join(root, "profile-#{cell}-#{instrument}-#{role}"), instrument, cell)
        rescue StandardError => error
          item = { 'valid' => false, 'instrument' => instrument, 'error' => error.message }
        end
        item.merge!('role' => role, 'cell' => cell)
        report['profiles'] << item
        save(File.join(root, 'report.json'), report)
        puts JSON.generate('cell' => cell, 'profile' => instrument, 'role' => role, 'valid' => item['valid'], 'error' => item['error'])
        $stdout.flush
      end
    rescue StandardError => error
      report.merge!('error' => error.message, 'complete' => false)
      save(File.join(root, 'report.json'), report)
      raise
    end
    check(report['complete'], 'paired diagnostic incomplete; all adverse rows retained')
  end
end

CheckpointPair.main if $PROGRAM_NAME == __FILE__

#!/usr/bin/env ruby
# Finite native Engine-open experiment. Preserve every planned outcome.
require_relative 'diagnostic_support'

module RecoveryAttribution
  extend DiagnosticSupport
  VARIANTS = %w[baseline key_index single_reader timers_off].freeze

  def self.parse_perf_ack(payload)
    check(["ack\n", "ack\n\0"].include?(payload), "unexpected sampler acknowledgement #{payload.inspect}")
    'ack'
  end

  def self.parse_perf_report(text)
    lost = text.scan(/^# Total Lost Samples: (\d+)\s*$/)
    samples = text.scan(/^# Samples: ([0-9.]+)([KMG]?) of event '[^']+'\s*$/)
    { 'lost_samples' => lost.size == 1 ? lost.first.first.to_i : nil,
      'samples' => samples.size == 1 ? (samples[0][0].to_f * { '' => 1, 'K' => 1_000, 'M' => 1_000_000, 'G' => 1_000_000_000 }.fetch(samples[0][1])).to_i : nil }
  end

  def self.run_plain(command, directory)
    run_process(command, out: File.join(directory, 'stdout.log'), err: File.join(directory, 'stderr.log'), env: { 'RUST_BACKTRACE' => '1' })
  end

  def self.run_cpu(command, directory)
    control, ack = %w[control.fifo ack.fifo].map { |name| File.join(directory, name) }
    [control, ack].each { |path| checked(['mkfifo', '-m', '600', path]) }
    ctl = File.open(control, File::RDWR | File::NONBLOCK)
    ack_io = File.open(ack, File::RDWR | File::NONBLOCK)
    perf = ['sudo', '-n', 'perf', 'record', '-F', '999', '-g', '--call-graph', 'dwarf', '--no-buildid', '--no-buildid-cache', '--delay=-1', "--control=fifo:#{control},#{ack}", '-o', File.join(directory, 'perf.data'), '--', 'runuser', '-u', ENV.fetch('USER', 'runner'), '--', 'env', 'MIDGE_RECOVERY_CPU_CONTROL=stdio', 'RUST_BACKTRACE=1', *command]
    receipt = { 'command' => perf, 'boundaries' => [], 'exit_code' => nil, 'error' => nil }
    save(File.join(directory, 'sampler.json'), receipt)
    pid = nil
    File.open(File.join(directory, 'stdout.log'), 'wb') do |out|
      File.open(File.join(directory, 'stderr.log'), 'wb') do |err|
        input_read, input_write = IO.pipe
        output_read, output_write = IO.pipe
        begin
          pid = Process.spawn(*perf, in: input_read, out: output_write, err: err, pgroup: true)
          input_read.close
          output_write.close
          pending = ''.b
          deadline = clock + 300
          loop do
            check(clock < deadline, 'fixed 300-second process bound exceeded')
            next unless IO.select([output_read], nil, nil, 0.1)
            chunk = output_read.read_nonblock(65_536, exception: false)
            break if chunk.nil?
            next if chunk == :wait_readable
            out.write(chunk)
            out.flush
            pending << chunk
            while (newline = pending.index("\n"))
              line = pending.slice!(0, newline + 1)
              next unless line.start_with?('MIDGE_PROFILE_BOUNDARY ')
              action = line.split.last
              expected = receipt['boundaries'].empty? ? 'enable' : 'disable'
              check(action == expected && receipt['boundaries'].size < 2, 'unexpected native sampling boundary')
              started = clock
              ctl.syswrite(action + "\n")
              check(IO.select([ack_io], nil, nil, 5), 'sampler did not acknowledge within five seconds')
              raw = ack_io.read_nonblock(4096)
              receipt['boundaries'] << { 'action' => action, 'ack' => parse_perf_ack(raw), 'ack_raw_hex' => raw.unpack1('H*'), 'elapsed_ns' => ((clock - started) * 1e9).to_i }
              save(File.join(directory, 'sampler.json'), receipt)
              input_write.write("CONTINUE\n")
              input_write.flush
            end
          end
          status = wait_bounded(pid, 5)
          pid = nil
          receipt['exit_code'] = status.exitstatus || -status.termsig
          check(receipt['boundaries'].map { |row| row['action'] } == %w[enable disable], 'both sampling boundaries required')
        rescue StandardError => error
          receipt['error'] = error.message
          status = terminate_group(pid, privileged: true) if pid
          receipt['exit_code'] = status && (status.exitstatus || -status.termsig)
        ensure
          [input_read, input_write, output_read, output_write].each { |io| io.close unless io.closed? }
        end
      end
    end
    data = File.join(directory, 'perf.data')
    if File.exist?(data)
      checked(['sudo', '-n', 'chown', "#{Process.uid}:#{Process.gid}", data])
      report = ['perf', 'report', '--stdio', '--no-children', '--no-inline', '-i', data]
      receipt['report_command'] = report
      result = run_process(report, out: File.join(directory, 'perf-report.txt'), err: File.join(directory, 'perf-report-error.log'), timeout: 60, env: { 'DEBUGINFOD_URLS' => '' })
      receipt['report_exit_code'] = result['exit_code']
      receipt['report_error'] = result['error'] if result['error']
      receipt.merge!(parse_perf_report(File.read(File.join(directory, 'perf-report.txt'))))
      receipt['data_sha256'] = digest(data)
    end
    save(File.join(directory, 'sampler.json'), receipt)
    receipt
  ensure
    [ctl, ack_io].compact.each { |io| io.close unless io.closed? }
    [control, ack].compact.each { |path| File.unlink(path) if File.exist?(path) }
  end

  def self.validate_process(outcome, directory)
    check(outcome['exit_code'] == 0 && !outcome['error'], 'native process or sampling control failed')
    check(!File.binread(File.join(directory, 'stderr.log')).include?('panicked at'), 'native process stderr contains Rust panic; retained native failure')
  end

  def self.validate_native(path, source, binary, fixture, variant)
    receipt = json(path)
    check(receipt['complete'] && receipt['source_clean'] && receipt['source_sha'] == source && receipt['binary_sha256'] == binary, 'incomplete or mismatched immutable-source receipt')
    result = receipt.fetch('result')
    check(result['fixture_sha256'] == fixture && result['variant'] == variant, 'trial input or mode differs')
    checks = result.fetch('verification')
    check(checks.size == 6 && checks.all? { |row| row['passed'] && row['value_mismatches'] == 0 && row['expected_rows'] == row['actual_rows'] } && result['shutdowns'] == 2, 'six exact checks and two shutdowns required')
    check(result.values_at('memory_bytes', 'local_storage_bytes', 'memtable_bytes', 'open_deadline_seconds') == [134_217_728, 1_073_741_824, 262_144, 30], 'resource/deadline policy differs')
    runtime = result.fetch('runtime')
    frontier = result.fetch('committed_wal_frontier')
    check(runtime['wal_cloud_durable_seq'] == frontier && runtime['current_sequence'] >= frontier && frontier <= result['frontier'] && runtime['salvage_mode_opens'] == 0, 'durable WAL frontier or salvage policy differs')
    events = result.fetch('native_open_events')
    facts = events.select { |event| event.key?('attribution') }.map { |event| event['attribution'] }
    coverage = events.select { |event| event['phase'] == 'coverage' }
    check(facts.size == 1 && coverage.size == 1, 'one native coverage owner required')
    fact = facts.first
    check(fact['variant'] == variant && fact['budget_final'] == 0 && fact['budget_peak'] <= fact['budget_limit'] && fact['peak_readers'] <= fact['reader_limit'] && fact['reader_limit'] <= 4, 'coverage resource/variant proof differs')
    %w[point_probes exact_hits checkpoint_releases index_builds].each { |name| check(fact.fetch(name) > (name == 'index_builds' ? 1 : 0), "fixture did not exercise #{name}") }
    check(coverage[0]['reader_evictions'] != 0 && (variant == 'key_index' || fact['predicate_rejections'] != 0), 'fixture did not exercise reader churn and predicate rejection')
    reads = fact.fetch('remote_ranges')
    check(fact['remote_observer_attached'] && reads['attempts'] > 0 && reads['attempts'] == reads['completed'] && reads['failures'] == 0, 'remote range receipts incomplete')
    result
  end

  def self.main
    output = 'target/recovery-attribution'
    OptionParser.new { |cli| cli.on('--output PATH') { |value| output = value } }.parse!
    check(ARGV.empty?, 'unexpected arguments')
    root = File.expand_path(output)
    check(!File.exist?(root), 'output directory already exists')
    FileUtils.mkdir_p(root)
    source = checked(%w[git rev-parse HEAD])
    build = %w[cargo build --release --example recovery_attribution --features internal-testing,failpoints --message-format=json-render-diagnostics]
    environment = { 'CARGO_PROFILE_RELEASE_DEBUG' => '1', 'CARGO_INCREMENTAL' => '0' }
    check(system(environment, *build, out: File.join(root, 'cargo.jsonl'), err: File.join(root, 'build.log')), 'native build failed')
    executable = File.readlines(File.join(root, 'cargo.jsonl')).map { |line| JSON.parse(line) }.select { |row| row['reason'] == 'compiler-artifact' && row.dig('target', 'name') == 'recovery_attribution' && row['executable'] }.last&.fetch('executable')
    check(executable, 'missing Cargo executable receipt')
    archived = File.join(root, 'binary', 'recovery_attribution')
    FileUtils.mkdir_p(File.dirname(archived))
    FileUtils.cp(executable, archived, preserve: true)
    binary = digest(archived)
    fixture, seed = %w[fixture seed].map { |name| File.join(root, name) }
    Dir.mkdir(seed)
    outcome = run_plain([archived, 'seed', fixture, File.join(seed, 'native.json')], seed)
    save(File.join(seed, 'process.json'), outcome)
    validate_process(outcome, seed)
    fixture_sha = json(File.join(fixture, 'fixture.json')).fetch('inventory').fetch('sha256')
    manifest = { 'source_sha' => source, 'binary_sha256' => binary, 'fixture_sha256' => fixture_sha, 'command' => build, 'binary_path' => 'binary/recovery_attribution', 'profile_release_debug' => 1, 'planned_plain' => 12, 'planned_cpu' => 4, 'trials' => [], 'complete' => false }
    save(File.join(root, 'campaign.json'), manifest)
    planned = (1..3).flat_map { |repeat| VARIANTS.rotate(repeat - 1).map { |variant| [variant, repeat, false] } } + VARIANTS.map { |variant| [variant, 1, true] }
    failures = []
    planned.each do |variant, repeat, sampled|
      name = "#{variant}-r#{repeat}-#{sampled ? 'cpu' : 'plain'}"
      directory = File.join(root, name)
      Dir.mkdir(directory)
      command = [archived, 'trial', fixture, File.join(directory, 'database'), variant, File.join(directory, 'native.json')]
      outcome = sampled ? run_cpu(command, directory) : run_plain(command, directory)
      row = { 'name' => name, 'variant' => variant, 'repeat' => repeat, 'sampled' => sampled, 'process' => outcome, 'accepted' => false }
      begin
        validate_process(outcome, directory)
        result = validate_native(File.join(directory, 'native.json'), source, binary, fixture_sha, variant)
        check(!sampled || (outcome['report_exit_code'] == 0 && outcome['lost_samples'] == 0 && outcome.fetch('samples', 0).to_i > 0), 'native profile report failed')
        row.merge!('open_ns' => result['open_ns'], 'target_met' => result['target_met'], 'accepted' => true)
      rescue StandardError => error
        row['error'] = error.message
        failures << name
      end
      manifest['trials'] << row
      save(File.join(root, 'campaign.json'), manifest)
      save(File.join(directory, 'process.json'), outcome)
      puts JSON.generate(row)
      $stdout.flush
    end
    manifest.merge!('complete' => failures.empty?, 'failures' => failures)
    save(File.join(root, 'campaign.json'), manifest)
    check(failures.empty?, "planned trials failed: #{failures.join(', ')}")
  end
end

RecoveryAttribution.main if $PROGRAM_NAME == __FILE__

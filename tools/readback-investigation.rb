#!/usr/bin/env ruby
# Literal receipt readback. This verifies reporting/correctness controls, not
# statistical confidence, engine policy acceptance or production-provider SLOs.
require 'json'
require 'csv'
require 'digest'

root, sha, seconds = ARGV
abort 'usage: readback-investigation.rb EXTRACTED_ARTIFACT_ROOT SHA SECONDS' unless root && sha && seconds
seconds = Integer(seconds)
raise 'invalid source/duration' unless sha.match?(/\A[0-9a-f]{40}\z/) && seconds.positive?
def check(condition, message)
  raise message unless condition
end
def json(path)
  JSON.parse(File.read(path))
end

statuses = Dir.glob(File.join(root, '**', 'workload-status.json'))
rows = statuses.map do |path|
  status = json(path)
  directory = File.dirname(path)
  check(status['git_commit'] == sha, "source mismatch: #{path}")
  check(status['source_worktree_clean'] && status.fetch('binary_sha256').match?(/\A[0-9a-f]{64}\z/), 'missing clean-source executable identity')
  identities = Dir.glob(File.join(File.dirname(directory), 'runner-logs', 'binary-*.sha256')).map { |p| File.read(p).strip.split(/\s+/, 2) }.select { |hash, _| hash == status['binary_sha256'] }
  check(identities.size == 1, 'runner/workload executable checksum mismatch')
  executable = identities.first[1]
  built = Dir.glob(File.join(File.dirname(directory), 'runner-logs', 'build-*.jsonl')).flat_map { |p| File.readlines(p).map { |l| JSON.parse(l) } }.any? { |d| d['reason'] == 'compiler-artifact' && d['executable'] == executable }
  check(built, 'executable missing from retained Cargo artifact receipt')
  check(status['status'] == 'passed' && status['phase'] == 'complete' && status['terminal_error'].nil?, "failed workload: #{path}")
  check(status['configured_duration_seconds'] == seconds, 'configured interval mismatch')
  check(status.dig('final_flush', 'completed'), 'incomplete final flush')
  check(status['shutdown_results'].size == 2 && status['shutdown_results'].all? { |s| s['caller_result'] == 'ok' && s['error'].nil? }, 'shutdown mismatch')
  final = status.fetch('finalization')
  check(final['sha'] == sha && final['step_outcome'] == 'success' && final['receipt_selection'] == 'matched' && !final['failed'] && !final['correctness_failed'] && final.fetch('benchmark_errors').empty?, 'native finalization failure')
  verification = json(File.join(directory, 'verification-summary.json'))
  check(verification['passed'] && verification['checks'].size == 6 && verification['checks'].all? { |c| c['passed'] && c['actual_rows'] == c['expected_rows'] && c['value_mismatches'].zero? }, 'exact verification failed')
  # Artifacts are extracted into one directory per original ZIP. Locate its
  # native report by source + workload; never choose a report from another ZIP.
  candidates = Dir.glob(File.join(root, '**', 'latest.json')).map { |p| [p, json(p)] }.select do |p, d|
    d.dig('environment', 'git_commit') == sha && d.fetch('benchmark_specs', []).any? { |b| b['id'].include?("::#{status['benchmark_workload']}/") } &&
      p.split('/target/').first == path.split('/target/').first
  end
  # upload-artifact strips target/ when retaining its common ancestor.
  candidates = Dir.glob(File.join(File.dirname(File.dirname(directory)), '**', 'latest.json')).map { |p| [p, json(p)] }.select { |_, d| d.dig('environment', 'git_commit') == sha && d.fetch('benchmark_specs', []).any? { |b| b['id'].include?("::#{status['benchmark_workload']}/") } } if candidates.empty?
  check(candidates.size == 1, "ambiguous native report: #{path}")
  report_path, report = candidates.first
  check(report.dig('environment', 'command_line', 0) == executable, 'native report executable differs from fingerprinted artifact')
  samples = report['samples'].select { |s| s['phase'] == 'measured' }
  check(samples.sum { |s| s['elapsed_ns'] } >= seconds * 1_000_000_000, 'actual interval too short')
  csv = CSV.read(File.join(directory, 'stages.csv'), headers: true)
  check(samples.size == csv.size, 'native/CSV stage count mismatch')
  samples.each_with_index do |sample, index|
    check(sample.fetch('operations_completed').positive?, 'no real successful completions')
    parameters = sample.fetch('parameters')
    values = csv[index]
    check(parameters['latency_accounting_valid'] == 'true' && values['valid'] == 'true', 'invalid latency accounting')
    %w[attempt successful_call inter_ack].each do |population|
      field = population == 'attempt' ? 'attempted_transactions' : 'acknowledged_transactions'
      count = Integer(parameters.fetch("#{population}_latency_samples"))
      check(count == Integer(parameters.fetch(field)), "#{population} sample cardinality mismatch")
      check(count == Integer(values.fetch("#{population}_samples")), 'CSV sample cardinality mismatch')
      observations = sample.fetch('observations').to_h { |o| [o['name'], o['value']] }
      %w[p50 p95 p99].each do |rank|
        check(observations["#{population == 'successful_call' ? 'successful_call' : population}_latency_#{rank}_us"] == Integer(values.fetch("#{population}_#{rank}_us")), 'CSV/native percentile mismatch')
      end
    end
    %w[actual_sleep_ns censored_inter_ack_samples censored_inter_ack_ns].each { |key| check(Integer(parameters.fetch(key)) == Integer(values.fetch(key)), 'CSV/native worker time mismatch') }
    before = json(File.join(directory, format('stage-%02d-prestate.json', index))).fetch('runtime')
    after = json(File.join(directory, format('stage-%02d-endstate.json', index))).fetch('runtime')
    admission_before = json(File.join(directory, format('stage-%02d-prestate.json', index)))['write_admission']
    admission_after = json(File.join(directory, format('stage-%02d-endstate.json', index)))['write_admission']
    if admission_before || admission_after
      check(admission_before && admission_after, 'incomplete admission endpoints')
      origin_sum = %w[queue l0 cloud_generation cloud_wal].sum do |reason|
        delta = admission_after.fetch("#{reason}_total") - admission_before.fetch("#{reason}_total")
        check(delta >= 0 && parameters.fetch("midge_admission_#{reason}_delta_valid") == 'true', 'admission counter reset')
        check(Integer(parameters.fetch("midge_admission_#{reason}_delta")) == delta, 'admission origin mismatch')
        delta
      end
      commits = admission_after.fetch('commit_write_stall_total') - admission_before.fetch('commit_write_stall_total')
      check(commits >= 0 && parameters.fetch('midge_admission_commit_delta_valid') == 'true', 'commit rejection counter reset')
      check(Integer(parameters.fetch('midge_admission_commit_delta')) == commits, 'commit rejection endpoint mismatch')
      check(commits == Integer(parameters.fetch('write_stall_responses')) && origin_sum == commits && parameters.fetch('midge_admission_counts_reconcile') == 'true', 'unattributed or duplicate rejection')
      {'flush_build_count'=>'flush_build_count', 'flush_build_ns'=>'flush_build_ns_total', 'flush_publish_count'=>'flush_publish_count', 'flush_publish_ns'=>'flush_publish_ns_total', 'compactions'=>'compactions_run', 'compaction_bytes'=>'compaction_bytes_rewritten', 'write_stall_ns'=>'write_stall_ns_total'}.each do |name, field|
        check(parameters.fetch("midge_#{name}_delta_valid") == 'true' && Integer(parameters.fetch("midge_#{name}_delta")) == after.fetch(field) - before.fetch(field), 'maintenance delta mismatch')
      end
    end
    %w[memory compaction cloud no_space].each do |reason|
      check(parameters["midge_write_stalls_#{reason}_delta_valid"] == 'true', 'runtime reason counter reset')
      check(Integer(parameters.fetch("midge_write_stalls_#{reason}_delta")) == after.fetch("write_stalls_#{reason}_total") - before.fetch("write_stalls_#{reason}_total"), 'stage-local stall delta mismatch')
      check(Integer(parameters.fetch("midge_write_stalls_#{reason}")) == after.fetch("write_stalls_#{reason}_total"), 'cumulative stall counter mismatch')
    end
  end
  latency = status.fetch('latency')
  check(latency['valid'] && latency['attempt_samples'] == status['attempted_transactions'] && latency['successful_call_samples'] == status['acknowledged_transactions'] && latency['inter_ack_samples'] == status['acknowledged_transactions'], 'status sample cardinality mismatch')
  %w[actual_sleep_ns censored_inter_ack_samples censored_inter_ack_ns].each { |key| check(latency[key] == csv.sum { |r| Integer(r[key]) }, 'status/CSV summed worker time mismatch') }
  if status['measurement_topology'] == 'independent_fresh_process'
    check(samples.size == 1, 'comparison inherited earlier stages')
    initial = json(File.join(directory, 'stage-00-prestate.json'))
    check(initial['isolated_comparison'] && initial['prior_stage_count'].zero? && initial['verified_initial_rows'] == initial['seed_rows'], 'comparison initial state mismatch')
  end
  { 'workload' => status['benchmark_workload'], 'binary_sha256' => status['binary_sha256'], 'comparison_repeat' => status['comparison_repeat'], 'topology' => status['measurement_topology'], 'clients' => samples.map { |s| s.dig('parameters', 'concurrent_clients') },
    'checks' => 6, 'shutdowns' => 2, 'measured_ns' => samples.sum { |s| s['elapsed_ns'] }, 'status_sha256' => Digest::SHA256.file(path).hexdigest, 'report_sha256' => Digest::SHA256.file(report_path).hexdigest }
end
check(!rows.empty?, 'no completed receipts')
puts JSON.pretty_generate({ 'git_commit' => sha, 'verified_cases' => rows.size, 'rows' => rows })

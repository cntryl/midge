#!/usr/bin/env ruby
# Finalize isolated workload artifacts from native receipts after process exit.
require_relative 'diagnostic_support'

module StressArtifacts
  extend DiagnosticSupport
  RUN_STEM = /\A\d+-(\d+)-\d+\z/
  COUNTERS = %w[attempted_transactions acknowledged_transactions acknowledged_rows logical_operations resource_limit_responses write_stall_responses saturation_backoff_ms].freeze

  def self.read_json(path)
    json(path)
  rescue SystemCallError, JSON::ParserError
    nil
  end

  def self.selected_workload(command)
    return unless command.is_a?(Array) && command.all? { |value| value.is_a?(String) }
    selections = command.each_cons(2).map { |flag, value| value if %w[--workload --filter].include?(flag) }.compact
    selections.concat(command.map { |value| value.split('=', 2).last if value.start_with?('--workload=', '--filter=') }.compact)
    selections.first if selections.size == 1
  end

  def self.valid_receipt(receipt)
    return false unless receipt.is_a?(Hash) && receipt.fetch('metadata', {}).is_a?(Hash)
    meta = receipt.fetch('metadata', {})
    return false if meta.key?('reporter_errors') && !meta['reporter_errors'].is_a?(String)
    %w[benchmark_specs summaries samples].all? do |key|
      rows = receipt[key]
      rows.is_a?(Array) && rows.all? do |row|
        next false unless row.is_a?(Hash) && row.fetch('metadata', {}).is_a?(Hash)
        metadata = row.fetch('metadata', {})
        next false unless %w[benchmark_error failure_kind].all? { |field| !metadata.key?(field) || metadata[field].is_a?(String) }
        if key == 'summaries'
          correctness = row.fetch('correctness', {})
          correctness.is_a?(Hash) && (!correctness.key?('passed') || [true, false].include?(correctness['passed']))
        elsif key == 'samples'
          counters = row.fetch('counters', {})
          counters.is_a?(Hash) && counters.values.all? { |value| value.is_a?(Integer) && value >= 0 }
        else
          true
        end
      end
    end
  end

  def self.matching_receipts(root, workload, suite, sha)
    Dir.glob(File.join(root, '**', '*.json')).sort.map do |path|
      stem = File.basename(path, '.json')
      match = RUN_STEM.match(stem)
      next unless match
      receipt = read_json(path)
      next unless valid_receipt(receipt)
      environment = receipt['environment']
      next unless environment.is_a?(Hash) && receipt['schema_version'] == 'cntryl-stress.v2' && receipt['suite'] == suite && receipt['started_at'] == stem && environment['git_commit'] == sha && selected_workload(environment.fetch('command_line', [])) == workload
      [Integer(match[1], 10), path, receipt]
    end.compact
  end

  def self.receipt_failure(receipt)
    metadata = (receipt['benchmark_specs'] + receipt['summaries']).map { |row| row.fetch('metadata', {}) }
    messages = metadata.map { |item| item['benchmark_error'] unless item['benchmark_error'].to_s.empty? }.compact.uniq.sort
    kinds = metadata.map { |item| item['failure_kind'] unless item['failure_kind'].to_s.empty? }.compact.uniq.sort
    correctness_failed = receipt['summaries'].any? { |row| row.dig('correctness', 'passed') == false }
    counters_failed = receipt['samples'].any? { |row| %w[failures timeouts duplicates dropped validation_errors].any? { |key| row.fetch('counters', {}).fetch(key, 0).positive? } }
    reporter = receipt.fetch('metadata', {})['reporter_errors']
    { 'failed' => !messages.empty? || !kinds.empty? || correctness_failed || counters_failed || !reporter.to_s.empty?, 'failure_kind' => kinds.size == 1 ? kinds.first : nil, 'benchmark_errors' => messages, 'correctness_failed' => correctness_failed || counters_failed, 'reporter_error' => reporter }
  end

  def self.partial_stage(path, status)
    stage = status['stage_index']
    return unless status['phase'] == 'workload' && stage.is_a?(Integer)
    snapshots, unreadable, clients = [], [], []
    Dir.glob(File.join(File.dirname(path), 'client-snapshots', format('stage-%02d', stage), 'client-*.json')).sort.each do |snapshot_path|
      snapshot = read_json(snapshot_path)
      unless snapshot.is_a?(Hash)
        unreadable << snapshot_path
        next
      end
      next unless snapshot['process_id'] == status['process_id'] && snapshot['stage_index'] == stage && snapshot['phase'] == 'workload'
      client = snapshot['client_index']
      if !client.is_a?(Integer) || clients.include?(client) || !COUNTERS.all? { |key| snapshot[key].is_a?(Integer) && snapshot[key] >= 0 }
        unreadable << snapshot_path
        next
      end
      clients << client
      snapshots << [snapshot_path, snapshot]
    end
    { 'stage_index' => stage, 'snapshot_count' => snapshots.size, 'counters' => snapshots.empty? ? nil : COUNTERS.to_h { |key| [key, snapshots.sum { |_, snapshot| snapshot[key] }] }, 'snapshot_paths' => snapshots.map(&:first), 'unreadable_snapshots' => unreadable }
  end

  def self.finalize(stress_root, artifact_root, workload, suite, sha, outcome)
    receipts = matching_receipts(stress_root, workload, suite, sha)
    finalized, errors = [], []
    Dir.glob(File.join(artifact_root, '**', 'workload-status.json')).sort.each do |path|
      status = read_json(path)
      next unless status.is_a?(Hash) && status['benchmark_workload'] == workload && status['git_commit'] == sha && status['process_id'].is_a?(Integer)
      matches = receipts.select { |pid, _, _| pid == status['process_id'] }
      selection = matches.size == 1 ? 'matched' : matches.empty? ? 'missing' : 'ambiguous'
      evidence = { 'step_outcome' => outcome, 'receipt_selection' => selection, 'sha' => sha, 'benchmark_workload' => workload, 'failure_kind' => nil }
      failed = outcome != 'success'
      native = nil
      if matches.size == 1
        _, receipt_path, receipt = matches.first
        native = receipt_failure(receipt)
        evidence.merge!(native).merge!('source' => 'cntryl-stress', 'receipt_path' => receipt_path, 'receipt_run_id' => receipt['started_at'])
        failed ||= native['failed']
      else
        evidence['source'] = 'github-actions'
      end
      if outcome == 'success' && (selection != 'matched' || status['status'] != 'passed' || failed)
        errors << path
        failed = true
        evidence['artifact_consistency_error'] = true
      end
      status['status_before_finalization'] = status['status'] unless status.key?('status_before_finalization')
      status['status'] = 'failed' if failed
      evidence['failed'] = failed
      evidence['failure_source'] = if native && native['failed'] then 'cntryl-stress' elsif outcome != 'success' then 'github-actions' elsif failed then 'artifact-finalizer' end
      status['finalization'] = evidence
      partial = partial_stage(path, status)
      status['partial_stage_counters'] = partial if partial
      save(path, status)
      finalized << path
    end
    errors << 'no matching workload status' if outcome == 'success' && finalized.empty?
    summary = { 'finalized' => finalized, 'consistency_errors' => errors, 'matching_receipts' => receipts.size, 'step_outcome' => outcome }
    summary.merge!('note' => 'No matching workload status; workload cause unknown', 'failure_kind' => nil) if finalized.empty?
    summary
  end

  def self.main
    options = { 'stress-root' => 'target/stress', 'artifact-root' => 'target/midge-stress' }
    parser = OptionParser.new do |cli|
      %w[stress-root artifact-root workload suite sha outcome].each { |key| cli.on("--#{key} VALUE") { |value| options[key] = value } }
    end
    parser.parse!
    check(ARGV.empty? && %w[workload suite sha outcome].all? { |key| options.key?(key) }, parser.to_s)
    check(%w[success failure cancelled skipped].include?(options['outcome']), 'invalid outcome')
    summary = finalize(*%w[stress-root artifact-root workload suite sha outcome].map { |key| options[key] })
    puts JSON.pretty_generate(summary)
    exit(summary['consistency_errors'].empty? ? 0 : 1)
  end
end

StressArtifacts.main if $PROGRAM_NAME == __FILE__

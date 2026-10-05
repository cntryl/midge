#!/usr/bin/env ruby
require 'minitest/autorun'
require 'tmpdir'
require 'fileutils'
require_relative 'validate-invariant-catalog'

# Constructed inventory controls only; these never execute the linked Rust.
class InvariantCatalogTest < Minitest::Test
  def setup
    @root = Dir.mktmpdir('midge-invariant-reference-')
    @source_path = File.join(@root, 'case.rs')
    File.write(@source_path, <<~'RUST')
      fn producer() {}
      #[test]
      fn should_check_actual_marker() {
          let braces = r###" } fn forged() { "###;
          /* } /* fn also_forged() {} */ { */
          let quoted = '}';
          assert_eq!(1, 1);
      }
      fn actual_helper() {
          assert!(true);
      }
      #[test]
      fn should_call_actual_helper() {
          actual_helper();
      }
    RUST
    @options = { root: @root, catalog: File.join(@root, 'catalog.md'),
                 evidence: File.join(@root, 'evidence.json'), map: File.join(@root, 'map.md'),
                 hashes: File.join(@root, 'hashes.json') }
    @catalog = "| ID | Invariant | Evidence |\n|---|---|---|\n" \
               "| ACCT-1 | Reference control. | [Evidence](invariant-evidence.md#acct-1) |\n"
    @map = "# Map\n\n## ACCT-1\n"
    @document = { 'rows' => [{ 'id' => 'ACCT-1', 'preconditions' => 'Constructed source.',
                              'uncovered' => 'No runtime proof.',
                              'production' => [{ 'path' => 'case.rs', 'line' => 1,
                                                 'symbol' => 'producer', 'kind' => 'function',
                                                 'marker' => 'fn producer() {}' }],
                              'tests' => [{ 'path' => 'case.rs', 'line' => 3,
                                            'symbol' => 'should_check_actual_marker',
                                            'claim' => 'Checks one assertion marker.',
                                            'scope' => 'Constructed inventory.',
                                            'assertions' => [{ 'line' => 7,
                                                              'marker' => 'assert_eq!(1, 1);' }] }] }] }
    @hashes = { 'case.rs' => Digest::SHA256.file(@source_path).hexdigest }
  end

  def teardown
    FileUtils.remove_entry(@root)
  end

  def result
    File.write(@options[:catalog], @catalog)
    File.write(@options[:evidence], JSON.generate(@document))
    File.write(@options[:map], @map)
    File.write(@options[:hashes], JSON.generate(@hashes))
    InvariantCatalog.validate(@options)
  end

  def rejects(fragment)
    actual = result
    refute actual['valid'], actual.inspect
    assert actual['errors'].any? { |error| error.include?(fragment) }, actual.inspect
  end

  def test_accepts_exact_source_without_treating_literal_braces_as_functions
    # Arrange / Act
    actual = result
    source = InvariantCatalog::Source.new(@source_path)
    # Assert
    assert actual['valid'], actual.inspect
    assert_equal 1, actual['assertion_markers']
    assert_empty source.definitions('forged')
    assert_empty source.definitions('also_forged')
    assert_equal 8, source.function('should_check_actual_marker')['end_line']
    refute actual['rust_tests_executed']
  end

  def test_rejects_duplicate_catalog_id
    @catalog += @catalog.lines.last
    rejects('duplicate invariant ID')
  end

  def test_rejects_duplicate_evidence_id
    @document['rows'] << @document['rows'].first.dup
    rejects('duplicate invariant ID')
  end

  def test_rejects_empty_or_missing_evidence
    original = @document['rows'].dup
    @document['rows'].clear
    rejects('catalog/evidence IDs differ')
    @document['rows'] = original
    assert result['valid'], 'restoring the removed evidence corrects the catalog'
  end

  def test_rejects_dangling_catalog_link
    @catalog = @catalog.sub('#acct-1', '#acct-2')
    rejects('catalog evidence links missing/dangling')
  end

  def test_rejects_missing_map_anchor
    @map = '# Empty map'
    rejects('map/evidence IDs differ')
  end

  def test_rejects_duplicate_map_anchor
    @map += "\n## ACCT-1\n"
    rejects('map/evidence IDs differ')
  end

  def test_rejects_missing_production_file
    @document['rows'].first['production'].first['path'] = 'missing.rs'
    rejects('missing/unsafe source path')
  end

  def test_rejects_missing_test_symbol
    # Arrange: rename a real source function while leaving its old evidence reference.
    renamed = 'should_check_renamed_marker'
    File.write(@source_path, File.read(@source_path).sub('should_check_actual_marker', renamed))
    @hashes['case.rs'] = Digest::SHA256.file(@source_path).hexdigest
    # Act / Assert: stale named evidence fails, then the corrected reference passes.
    rejects('missing/ambiguous test symbol')
    @document['rows'].first['tests'].first['symbol'] = renamed
    assert result['valid'], 'correcting the renamed evidence restores validity'
  end

  def test_rejects_missing_production_symbol
    @document['rows'].first['production'].first['symbol'] = 'missing'
    rejects('missing production symbol')
  end

  def test_rejects_drifted_or_outside_assertion_marker
    @document['rows'].first['tests'].first['assertions'].first['line'] = 10
    rejects('assertion marker drift/outside test')
  end

  def test_rejects_missing_assertion_markers
    @document['rows'].first['tests'].first['assertions'].clear
    rejects('no assertion markers')
  end

  def test_rejects_source_postimage_drift
    File.open(@source_path, 'a') { |file| file.puts '// Source changed.' }
    rejects('source postimage changed')
  end

  def test_accepts_called_helper_but_rejects_comment_only_reference
    # Arrange
    reference = @document['rows'].first['tests'].first
    reference['line'] = 13
    reference['symbol'] = 'should_call_actual_helper'
    reference['assertions'] = [{ 'line' => 10, 'marker' => 'assert!(true);',
                                 'helper' => 'actual_helper' }]
    # Act / Assert
    assert result['valid']
    source = File.read(@source_path).sub('    actual_helper();', '    // actual_helper();')
    File.write(@source_path, source)
    @hashes['case.rs'] = Digest::SHA256.file(@source_path).hexdigest
    rejects('assertion helper is not referenced')
  end
end

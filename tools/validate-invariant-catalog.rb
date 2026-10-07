#!/usr/bin/env ruby
# Source/reference lint only. This does not execute Rust or establish an invariant.
require 'json'
require 'digest'
require 'optparse'
require 'pathname'

module InvariantCatalog
  ID = /\A[A-Z]+-[1-9][0-9]*\z/

  # Mask comments and Rust literals so braces inside them cannot end a function.
  # Preserve byte offsets/newlines; Rust test symbols used here are ASCII.
  def self.mask(source)
    input = source.b
    output = input.dup
    index = 0
    while index < input.bytesize
      finish = nil
      if input.byteslice(index, 2) == '//'
        finish = input.index("\n", index) || input.bytesize
      elsif input.byteslice(index, 2) == '/*'
        depth = 1
        cursor = index + 2
        while cursor < input.bytesize && depth > 0
          pair = input.byteslice(cursor, 2)
          if pair == '/*'
            depth += 1
            cursor += 2
          elsif pair == '*/'
            depth -= 1
            cursor += 2
          else
            cursor += 1
          end
        end
        finish = cursor
      elsif [98, 114].include?(input.getbyte(index)) &&
            (raw = /\A(?:br|r)(\#*)"/.match(input.byteslice(index, 40)))
        delimiter = '"' + raw[1]
        closing = input.index(delimiter, index + raw[0].bytesize)
        finish = closing ? closing + delimiter.bytesize : input.bytesize
      elsif input.getbyte(index) == 34
        cursor = index + 1
        while cursor < input.bytesize
          if input.getbyte(cursor) == 92
            cursor += 2
          elsif input.getbyte(cursor) == 34
            cursor += 1
            break
          else
            cursor += 1
          end
        end
        finish = cursor
      elsif input.getbyte(index) == 39 &&
            (character = /\A'(?:\\(?:u\{[0-9a-fA-F]+\}|x[0-9a-fA-F]{2}|.)|[^\\'\n])'/.match(input.byteslice(index, 20)))
        finish = index + character[0].bytesize
      end
      if finish
        length = finish - index
        output[index, length] = input.byteslice(index, length).gsub(/[^\n]/, ' ')
        index = finish
      else
        index += 1
      end
    end
    output
  end

  class Source
    attr_reader :path, :text, :lines, :masked

    def initialize(path)
      @path = path
      @text = File.read(path)
      @lines = @text.lines
      @masked = InvariantCatalog.mask(@text)
    end

    def definitions(symbol, kind = 'function')
      name = symbol.split('::').last
      prefix = kind == 'function' ? 'fn' : '(?:struct|enum|trait|type|const|static)'
      pattern = /\b#{prefix}\s+#{Regexp.escape(name)}\b/
      masked.to_enum(:scan, pattern).map do
        start = Regexp.last_match.begin(0)
        { 'line' => masked.byteslice(0, start).count("\n") + 1, 'offset' => start }
      end
    end

    def function(symbol, near = nil)
      found = definitions(symbol)
      return nil if found.empty?
      chosen = near ? found.min_by { |entry| (entry['line'] - near).abs } : found.first
      opening = masked.index('{', chosen['offset'])
      return nil unless opening
      depth = 1
      cursor = opening + 1
      while cursor < masked.bytesize && depth > 0
        byte = masked.getbyte(cursor)
        depth += 1 if byte == 123
        depth -= 1 if byte == 125
        cursor += 1
      end
      return nil unless depth.zero?
      chosen.merge('end_line' => masked.byteslice(0, cursor).count("\n") + 1,
                   'body' => text.b.byteslice(opening, cursor - opening),
                   'masked_body' => masked.byteslice(opening, cursor - opening))
    end
  end

  def self.validate(options)
    errors = []
    catalog = File.read(options.fetch(:catalog))
    document = JSON.parse(File.read(options.fetch(:evidence)))
    rows = document.fetch('rows')
    ids = catalog.scan(/^\| ([A-Z]+-[0-9]+) \|/).flatten
    evidence_ids = rows.map { |row| row.fetch('id') }
    errors << 'catalog/evidence inventory is empty' if ids.empty? || rows.empty?
    [ids, evidence_ids].each do |values|
      errors << 'duplicate invariant ID' unless values.uniq == values
      errors << 'invalid invariant ID' unless values.all? { |id| ID.match?(id) }
    end
    errors << 'catalog/evidence IDs differ' unless ids.sort == evidence_ids.sort
    evidence_map = File.read(options.fetch(:map))
    anchors = evidence_map.scan(/^## ([A-Z]+-[0-9]+)$/).flatten
    errors << 'map/evidence IDs differ' unless anchors.sort == evidence_ids.sort && anchors.uniq == anchors
    links = catalog.scan(/invariant-evidence\.md#([a-z]+-[0-9]+)/).flatten
    errors << 'catalog evidence links missing/dangling' unless links.sort == evidence_ids.map(&:downcase).sort
    root = Pathname.new(options.fetch(:root)).realpath
    cache = {}
    source_for = lambda do |relative|
      path = root.join(relative)
      unless path.file? && path.realpath.to_s.start_with?(root.to_s + File::SEPARATOR)
        errors << "missing/unsafe source path: #{relative}"
        next nil
      end
      cache[relative] ||= Source.new(path.to_s)
    end
    assertion_count = 0
    test_count = 0
    rows.each do |row|
      id = row['id']
      %w[preconditions uncovered].each do |field|
        errors << "#{id}: empty #{field}" if row[field].to_s.strip.empty?
      end
      errors << "#{id}: no production/evidence references" if row.fetch('production', []).empty? || row.fetch('tests', []).empty?
      row.fetch('production', []).each do |reference|
        source = source_for.call(reference.fetch('path'))
        next unless source
        line = reference.fetch('line')
        marker = reference.fetch('marker')
        errors << "#{id}: production marker drift #{reference['path']}:#{line}" unless source.lines[line - 1]&.strip == marker
        if reference['kind'] != 'text'
          declarations = source.definitions(reference.fetch('symbol'), reference.fetch('kind'))
          errors << "#{id}: missing production symbol #{reference['symbol']}" unless declarations.any? { |entry| entry['line'] == line }
        end
      end
      row.fetch('tests', []).each do |reference|
        source = source_for.call(reference.fetch('path'))
        next unless source
        test_count += 1
        declarations = source.definitions(reference.fetch('symbol'))
        errors << "#{id}: missing/ambiguous test symbol #{reference['symbol']}" unless declarations.size == 1
        function = source.function(reference.fetch('symbol'), reference.fetch('line'))
        next unless function
        errors << "#{id}: test line drift #{reference['symbol']}" unless function['line'] == reference['line']
        errors << "#{id}: empty test scope/evidence claim" if reference['scope'].to_s.strip.empty? || reference['claim'].to_s.strip.empty?
        assertions = reference.fetch('assertions', [])
        errors << "#{id}: no assertion markers #{reference['symbol']}" if assertions.empty?
        assertions.each do |assertion|
          assertion_count += 1
          line = assertion.fetch('line')
          marker = assertion.fetch('marker')
          owner = function
          if assertion['helper']
            helper = assertion.fetch('helper')
            owner = source.function(helper)
            errors << "#{id}: missing/ambiguous assertion helper #{helper}" unless source.definitions(helper).size == 1
            errors << "#{id}: assertion helper is not referenced by test #{reference['symbol']}" unless function['masked_body'].match?(/\b#{Regexp.escape(helper)}\b/)
          end
          owned = owner && (owner['line']..owner['end_line']).cover?(line)
          errors << "#{id}: assertion marker drift/outside test #{reference['symbol']}:#{line}" unless owned && source.lines[line - 1]&.strip == marker
        end
      end
    end
    hashes = JSON.parse(File.read(options.fetch(:hashes)))
    errors << 'source hash inventory differs from references' unless hashes.keys.sort == cache.keys.sort
    cache.each do |relative, source|
      errors << "source postimage changed: #{relative}" unless hashes[relative] == Digest::SHA256.hexdigest(source.text)
    end
    { 'valid' => errors.empty?, 'errors' => errors.uniq, 'invariant_rows' => ids.size,
      'test_references' => test_count, 'assertion_markers' => assertion_count,
      'source_files' => cache.size, 'rust_tests_executed' => false,
      'scope' => 'source/reference/assertion-marker lint; no semantic or execution proof' }
  end
end

if $PROGRAM_NAME == __FILE__
  options = { root: Dir.pwd, catalog: 'docs/development/invariant-catalog.md',
              evidence: 'docs/development/invariant-evidence.json',
              map: 'docs/development/invariant-evidence.md',
              hashes: 'docs/development/invariant-evidence-source-sha256.json' }
  OptionParser.new do |parser|
    parser.banner = 'Usage: ruby tools/validate-invariant-catalog.rb [paths]'
    options.keys.each { |key| parser.on("--#{key} PATH") { |value| options[key] = value } }
  end.parse!
  begin
    result = InvariantCatalog.validate(options)
    puts JSON.pretty_generate(result)
    exit(result['valid'] ? 0 : 1)
  rescue KeyError, JSON::ParserError, SystemCallError => error
    warn "invariant catalog input invalid: #{error.message}"
    exit 2
  end
end

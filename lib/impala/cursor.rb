module Impala
  # Cursors are used to iterate over result sets without loading them all
  # into memory at once. This can be useful if you're dealing with lots of
  # rows. It implements Enumerable, so you can use each/select/map/etc.
  class Cursor
    BUFFER_SIZE = 1024
    include Enumerable

    def self.typecast_boolean(value)
      value == 'true'
    end

    def self.typecast_int(value)
      value.to_i
    end

    def self.typecast_float(value)
      value.to_f
    end

    def self.typecast_decimal(value)
      BigDecimal.new(value)
    end

    def self.typecast_timestamp(value)
      Time.parse(value)
    end

    TYPECAST_MAP = {
      'boolean'=>method(:typecast_boolean),
      'int'=>method(:typecast_int),
      'double'=>method(:typecast_float),
      'decimal'=>method(:typecast_decimal),
      'timestamp'=>method(:typecast_timestamp),
    }
    TYPECAST_MAP['tinyint'] = TYPECAST_MAP['smallint'] = TYPECAST_MAP['bigint'] = TYPECAST_MAP['int']
    TYPECAST_MAP['float'] = TYPECAST_MAP['double']
    TYPECAST_MAP.freeze

    NULL = 'NULL'.freeze

    attr_reader :typecast_map

    attr_reader :handle, :service

    def initialize(handle, service, options = {})
      @handle = handle
      @service = service
      @loggers = options.fetch(:loggers, [])

      @row_buffer = []
      @done = false
      @open = true
      @typecast_map = TYPECAST_MAP.dup
      @options = options.dup
      @cancel_if = options.delete(:cancel_if)
      @progress_reporter = ProgressReporter.new(self, @options)
      @poll_every = options.fetch(:poll_every, 0.5)
      @log_every = options.fetch(:log_every, 1000)
    end

    def columns
      @columns ||= metadata.schema.fieldSchemas.map(&:name)
    end

    def inspect
      "#<#{self.class}#{handle ? " QueryID: #{handle.id}" : ''}#{open? ? '' : ' (CLOSED)'}>"
    end

    def each
      while row = fetch_row
        yield row
      end
    end

    # Returns the next available row as a hash, or nil if there are none left.
    # @return [Hash, nil] the next available row, or nil if there are none
    #    left
    # @see #fetch_all
    def fetch_row
      if @row_buffer.empty?
        if @done
          return nil
        else
          fetch_more
        end
      end

      @row_buffer.shift
    end

    # Returns all the remaining rows in the result set.
    # @return [Array<Hash>] the remaining rows in the result set
    # @see #fetch_one
    def fetch_all
      self.to_a
    end

    # Close the cursor on the remote server. Once a cursor is closed, you
    # can no longer fetch any rows from it.
    def close
      @open = false
      @service.close(@handle)
    end

    # Returns true if the cursor is still open.
    def open?
      @open
    end

    # Returns true if the query is done running, and results can be fetched.
    def query_done?
      @number_of_calls_to_query_done += 1

      state = @service.get_state(@handle)

      log_query_done_was_called(state) if should_log_query_done?

      if state != @last_value_from_get_state
        log_debug("get_state changed from #{@last_value_from_get_state} to #{state}")
        @last_value_from_get_state = state
      end

      unless Protocol::Beeswax::QueryState::VALID_VALUES.include?(state)
        log_debug("State is #{state} which is not one of the expected values: #{Protocol::Beeswax::QueryState::VALID_VALUES.inspect}")
      end

      [
        Protocol::Beeswax::QueryState::EXCEPTION,
        Protocol::Beeswax::QueryState::FINISHED
      ].include?(state)
    end

    def log_query_done_was_called(state)
      log_debug("Still polling Impala via get_state. Current state is: #{state}")
    end

    def should_log_query_done?
      return false if @log_every.zero?
      return (@number_of_calls_to_query_done % @log_every) == 0
    end

    # Blocks until the query done running.
    def wait!
      @number_of_calls_to_query_done = 0
      @last_value_from_get_state = -1
      until query_done?
        check_cancel
        periodic_callback
        sleep @poll_every
      end
      check_errors
    end

    # Returns true if there are any more rows to fetch.
    def has_more?
      !@done || !@row_buffer.empty?
    end

    def runtime_profile
      @service.GetRuntimeProfile(@handle)
    end

    def exec_summary
      @service.GetExecSummary(@handle)
    end

    # Returns the progress for the query.
    def progress
      summary = exec_summary
      summary.progress.num_completed_scan_ranges.to_f / summary.progress.total_scan_ranges.to_f
    end

    private

    attr :progress_reporter

    def periodic_callback
      return unless progress_reporter.show?
      progress_reporter.report
    end

    def metadata
      @metadata ||= @service.get_results_metadata(@handle)
    end

    def fetch_more
      fetch_batch until @done || @row_buffer.count >= BUFFER_SIZE
    end

    def check_cancel
      if @cancel_if && @cancel_if.call == true
        close
        raise CursorError.new("Cursor was closed due to :cancel_if returning true")
      end
    end

    def check_errors
      raise CursorError.new("Cursor has expired or been closed") unless @open
      raise ConnectionError.new("The query was aborted") if exceptional?
    end

    def exceptional?
      @service.get_state(@handle) == Protocol::Beeswax::QueryState::EXCEPTION
    end

    def fetch_batch
      check_cancel
      check_errors

      begin
        res = @service.fetch(@handle, false, BUFFER_SIZE)
      rescue Protocol::Beeswax::BeeswaxException
        @open = false
        raise CursorError.new("Cursor has expired or been closed")
      end

      rows = res.data.map { |raw| parse_row(raw) }
      @row_buffer.concat(rows)

      unless res.has_more
        @done = true
        close
      end
    end

    def parse_row(raw)
      row = {}
      fields = raw.split(metadata.delim)

      row_convertor.each do |c, p, i|
        v = fields[i]
        row[c] = (p ? p.call(v) : v unless v == NULL)
      end

      row
    end

    def row_convertor
      @row_convertor ||= columns.zip(metadata.schema.fieldSchemas.map{|s| typecast_map[s.type]}, (0...(columns.length)).to_a)
    end

    def log_debug(message)
      @loggers.each do |logger|
        logger.debug(message)
      end
    end
  end
end

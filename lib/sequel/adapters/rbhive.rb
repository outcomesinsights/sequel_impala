require 'rbhive'
require 'sequel/adapters/shared/impala'

module Sequel
  module Rbhive
    class Database < Sequel::Database
      include Impala::DatabaseMethods

      NullLogger = Object.new
      def NullLogger.info(str)
        nil
      end

      to_i = lambda(&:to_i)
      CONVERSION_PROCS = [
        nil,  #  0 => %q"BOOLEAN",
        nil,  #  1 => %q"TINYINT",
        nil,  #  2 => %q"SMALLINT",
        nil,  #  3 => %q"INT",
        nil,  #  4 => %q"BIGINT",
        nil,  #  5 => %q"FLOAT",
        nil,  #  6 => %q"DOUBLE",
        nil,  #  7 => %q"STRING",
        nil,  #  8 => %q"TIMESTAMP",
        nil,  #  9 => %q"BINARY",
        nil,  #  10 => %q"ARRAY",
        nil,  #  11 => %q"MAP",
        nil,  #  12 => %q"STRUCT",
        nil,  #  13 => %q"UNIONTYPE",
        lambda{|v| BigDecimal.new(v)},  #  15 => %q"DECIMAL",
        nil,  #  16 => %q"NULL",
        lambda{|v| Date.new(*v[0...10].split('-'))},  #  17 => %q"DATE",
        nil,  #  18 => %q"VARCHAR",
        nil,  #  19 => %q"CHAR",
      ]

      attr_reader :conversion_procs

      # Exception classes used by Impala.
      RbhiveExceptions = [
        RBHive::TCLIConnectionError,
        ::Thrift::TransportException,
        IOError
      ].freeze

      DisconnectExceptions = [
        ::Thrift::TransportException,
        IOError
      ].freeze

      set_adapter_scheme :rbhive

      # Connect to the Impala server.  Currently, only the :host and :port options
      # are respected, and they default to 'localhost' and 21000, respectively.
      def connect(server)
        opts = server_opts(server)
        opts[:hive_version] ||= 12
        conn = RBHive::TCLIConnection.new(opts[:host]||'localhost', opts[:port]||21050, opts, opts[:hive_logger] || NullLogger)
        conn.open
        conn.open_session
        conn
      end

      def database_error_classes
        RbhiveExceptions
      end

      def disconnect_connection(connection)
        connection.close_session if connection.session
        connection.close
      rescue *DisconnectExceptions
      end

      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |c|
          begin
            puts sql
            r = log_yield(sql){c.execute(sql)}
            yield(c, r) if block_given?
            nil
          rescue *RbhiveExceptions => e
            raise_error(e)
          end
        end
      end

      private

      def adapter_initialize
        @conversion_procs = CONVERSION_PROCS.dup
        @conversion_procs[8] = method(:to_application_timestamp)
      end

      def to_application_timestamp(v)
        return nil if v.nil?
        super
      end

      def connection_execute_method
        :execute
      end

      # Impala raises IOError if it detects a problem on the connection, and
      # in most cases that results in an unusable connection, so treat it as a
      # disconnect error so Sequel will reconnect.
      def disconnect_error?(exception, opts)
        case exception
        when *DisconnectExceptions
          true
        else
          super
        end
      end

      # Use DESCRIBE to get the column names and types for the table.
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth(opts[:dataset])

        table = if opts[:schema]
          Sequel.qualify(opts[:schema], table_name)
        else
          Sequel.identifier(table_name)
        end

        describe(table, opts).map do |row|
          row[:db_type] = row[:type]
          row[:type] = schema_column_type(row[:db_type])
          row[:default] = nil
          row[:primary_key] = false
          [m.call(row.delete(:name)), row]
        end
      end
    end

    class Dataset < Sequel::Dataset
      include Impala::DatasetMethods

      Database::DatasetClass = self

      APOS = "'".freeze
      STRING_ESCAPES = {
        "\\" => "\\\\".freeze,
        "'" => "\\'".freeze,
        "\n" => "\\n".freeze,
        "\r" => "\\r".freeze,
        "\0" => "\\0".freeze,
        "\b" => "\\b".freeze,
        "\04" => "\\Z".freeze,
       # Impala is supposed to support this, but using it
       # breaks things to the point of returning bad data.
       # If you don't do this, the tabs in the input
       # get converted to spaces, but that's better than the
       # alternative.
       # "\t" => "\\t".freeze,
      }.freeze
      STRING_ESCAPE_RE = /(#{Regexp.union(STRING_ESCAPES.keys)})/

      def fetch_rows(sql)
        execute(sql) do |conn, result|
          op_handle = result.operationHandle
          columns, type_nums = conn.get_column_info(op_handle)
          @columns = columns.map!{|c| output_identifier(c)}
          conversion_procs = db.conversion_procs
          convertors = conversion_procs.values_at(*type_nums)
          #cursor.typecast_map['timestamp'] = db.method(:to_application_timestamp)
          conn.yield_hash_rows(op_handle, columns, convertors) do |row|
            yield row
          end
        end
      end

      private

      def literal_string_append(sql, s)
        sql << APOS << s.to_s.gsub(STRING_ESCAPE_RE){|m| STRING_ESCAPES[m]} << APOS
      end
    end
  end
end


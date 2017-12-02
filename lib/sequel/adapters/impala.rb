require 'impala'
require 'sequel/adapters/shared/impala'

module Sequel
  module Impala
    class Database < Sequel::Database
      include DatabaseMethods

      # Exception classes used by Impala.
      ImpalaExceptions = [
        ::Impala::InvalidQueryError,
        ::Impala::ConnectionError,
        ::Impala::CursorError,
        ::Impala::ParsingError,
        ::Impala::Protocol::Beeswax::BeeswaxException,
        ::Thrift::TransportException,
        IOError
      ].freeze

      DisconnectExceptions = [
        ::Thrift::TransportException,
        IOError
      ].freeze

      set_adapter_scheme :impala

      # Connect to the Impala server.  Currently, only the :host and :port options
      # are respected, and they default to 'localhost' and 21000, respectively.
      def connect(server)
        opts = server_opts(server)
        ::Impala.connect(opts[:host]||'localhost', (opts[:port]||21000).to_i, opts)
      end

      def database_error_classes
        ImpalaExceptions
      end

      def disconnect_connection(c)
        c.close
      rescue *DisconnectExceptions
      end

      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |c|
          begin
            cursor = record_query_id(opts) do
              log_connection_yield(sql, c) do
                c.execute(sql){}
              end
            end
            yield cursor if block_given?
            nil
          rescue *ImpalaExceptions => e
            raise_error(e)
          ensure
            record_profile(cursor, opts)
            cursor.close if cursor && cursor.open?
          end
        end
      end

      def profile_for(profile_name=:default)
        Sequel.synchronize{@runtime_profiles[profile_name]}
      end

      def query_id_for(query_id_name=:default)
        Sequel.synchronize{@query_ids[query_id_name]}
      end

      private

      def dataset_class_default
        Dataset
      end

      def record_profile(cursor, opts)
        if cursor && profile_name = opts[:profile_name]
          profile = cursor.runtime_profile
          Sequel.synchronize{@runtime_profiles[profile_name] = profile}
        end
      end

      def record_query_id(opts = OPTS)
        start = Time.now if opts[:query_id_name]

        cursor = yield

        if cursor && query_id_name = opts[:query_id_name]
          Sequel.synchronize{ @query_ids[query_id_name] = { query_id: cursor.handle.id, start_time: start } }
        end

        cursor
      end

      def adapter_initialize
        @runtime_profiles = {}
        @query_ids = {}
      end

      def connection_execute_method
        :query
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
      include DatasetMethods

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
        execute(sql, @opts) do |cursor|
          self.columns = cursor.columns.map!{|c| output_identifier(c)}
          cursor.typecast_map['timestamp'] = db.method(:to_application_timestamp)
          cursor.each do |row|
            yield row
          end
        end

        self
      end

      def profile(profile_name=:default)
        clone(:profile_name => profile_name)
      end

      def query_id(query_id_name=:default)
        clone(:query_id_name => query_id_name)
      end

      private

      # Unlike the jdbc/hive2 driver, the impala driver requires you escape
      # some values in string literals to get correct results, but not the
      # tab character or things break.
      def literal_string_append(sql, s)
        sql << APOS << s.to_s.gsub(STRING_ESCAPE_RE){|m| STRING_ESCAPES[m]} << APOS
      end
    end
  end
end

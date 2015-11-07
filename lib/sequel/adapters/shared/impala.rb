module Sequel
  module Impala
    module Invalid
      def invalid(meth, msg)
        define_method(meth) do |*|
          raise InvalidOperation, msg
        end
      end
    end

    module DatabaseMethods
      extend Invalid

      def database_type
        :impala
      end

      def supports_create_table_if_not_exists?
        true
      end

      def transaction(opts=OPTS)
        synchronize(opts[:server]) do |c|
          yield c
        end
      end

      def serial_primary_key_options
        {:type=>Integer}
      end

      private

      def identifier_input_method_default
        nil
      end
     
      def identifier_output_method_default
        nil
      end

      def type_literal_generic_string(column)
        'string'
      end
    end

    module DatasetMethods
      extend Invalid

      BACKTICK = '`'.freeze
      APOS = "'".freeze
      STRING_ESCAPE_RE = /([\\'])/
      STRING_ESCAPE_REPLACE = '\\\\\1'.freeze
      BOOL_TRUE = 'true'.freeze
      BOOL_FALSE = 'false'.freeze

      invalid :update, "Impala does not support UPDATE"
      invalid :delete, "Impala does not support DELETE"
      invalid :truncate, "Impala does not support TRUNCATE or DELETE"

      def supports_cte?(type=:select)
        true
      end
      
      def supports_derived_column_lists?
        false
      end

      def insert_supports_empty_values?
        false
      end

      def supports_intersect_except?
        false
      end

      def supports_is_true?
        false
      end
    
      def supports_window_functions?
        true
      end

      private

      def insert_empty_columns_values
        [[columns.last], [nil]]
      end
    
      def literal_true
        BOOL_TRUE
      end

      def literal_false
        BOOL_FALSE
      end

      def literal_string_append(sql, s)
        sql << APOS << s.to_s.gsub(STRING_ESCAPE_RE, STRING_ESCAPE_REPLACE) << APOS 
      end

      def quoted_identifier_append(sql, name)
        sql << BACKTICK << name.to_s << BACKTICK
      end

      def select_limit_sql(sql)
        return unless opts[:from]
        super
      end
    end
  end
end

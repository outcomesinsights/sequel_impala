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
      CONSTANT_LITERAL_MAP = {:CURRENT_TIMESTAMP=>'now()'.freeze}.freeze
      PAREN_OPEN = Dataset::PAREN_OPEN
      PAREN_CLOSE = Dataset::PAREN_CLOSE
      SPACE = Dataset::SPACE
      NOT = 'NOT '.freeze
      REGEXP = ' REGEXP '.freeze

      invalid :update, "Impala does not support UPDATE"
      invalid :delete, "Impala does not support DELETE"
      invalid :truncate, "Impala does not support TRUNCATE or DELETE"

      def complex_expression_sql_append(sql, op, args)
        case op
        when :'||'
          literal_append(sql, Sequel.function(:concat, *args))
        when :LIKE, :'NOT LIKE'
          sql << PAREN_OPEN
          literal_append(sql, args.at(0))
          sql << SPACE << op.to_s << SPACE
          literal_append(sql, args.at(1))
          sql << PAREN_CLOSE
        when :~, :'!~', :'~*', :'!~*'
          if op == :'~*'  || op == :'!~*'
            args = args.map{|a| Sequel.function(:upper, a)}
          end
          sql << NOT if op == :'!~'  || op == :'!~*'
          sql << PAREN_OPEN
          literal_append(sql, args.at(0))
          sql << REGEXP
          literal_append(sql, args.at(1))
          sql << PAREN_CLOSE
        else
          super
        end
      end

      def constant_sql_append(sql, constant)
        sql << CONSTANT_LITERAL_MAP.fetch(constant, constant.to_s)
      end

      def empty?
        get(Sequel::SQL::AliasedExpression.new(1, :one)).nil?
      end

      def insert_supports_empty_values?
        false
      end

      def supports_cte?(type=:select)
        true
      end
      
      def supports_derived_column_lists?
        false
      end

      def supports_intersect_except?
        false
      end

      def supports_is_true?
        false
      end
    
      def supports_multiple_column_in?
        false
      end

      def supports_regexp?
        true
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

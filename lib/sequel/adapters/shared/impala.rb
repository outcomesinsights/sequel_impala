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

      def create_join_table(hash, options=OPTS)
        keys = hash.keys.sort_by(&:to_s)
        create_table(join_table_name(hash, options), options) do
          keys.each do |key|
            Integer key
          end
        end
      end

      def database_type
        :impala
      end

      def serial_primary_key_options
        {:type=>Integer}
      end

      def supports_create_table_if_not_exists?
        true
      end

      def supports_foreign_key_parsing?
        false
      end

      def supports_index_parsing?
        false
      end

      def views(opts=OPTS)
        tables(opts)
      end

      def transaction(opts=OPTS)
        synchronize(opts[:server]) do |c|
          yield c
        end
      end

      private

      def alter_table_add_column_sql(table, op)
        "ADD COLUMNS (#{column_definition_sql(op)})"
      end

      def alter_table_change_column_sql(table, op)
        o = op[:op]
        opts = schema(table).find{|x| x.first == op[:name]}
        opts = opts ? opts.last.dup : {}
        opts[:name] = o == :rename_column ? op[:new_name] : op[:name]
        opts[:type] = o == :set_column_type ? op[:type] : opts[:db_type]
        opts.delete(:primary_key)
        opts.delete(:default)
        opts.delete(:null)
        opts.delete(:allow_null)
        unless op[:type] || opts[:type]
          raise Error, "cannot determine database type to use for CHANGE COLUMN operation"
        end
        opts = op.merge(opts)
        "CHANGE #{quote_identifier(op[:name])} #{column_definition_sql(opts)}"
      end
      alias alter_table_rename_column_sql alter_table_change_column_sql
      alias alter_table_set_column_type_sql alter_table_change_column_sql

      # DDL statement for creating a table from the result of a SELECT statement.
      # +sql+ should be a string representing a SELECT query.
      def create_table_as_sql(name, sql, options)
        "#{create_table_prefix_sql(name, options)}#{create_table_parameters_sql(options) } AS #{sql}"
      end

      # DDL fragment for initial part of CREATE TABLE statement
      def create_table_prefix_sql(name, options)
        "CREATE #{'EXTERNAL ' if options[:external]}TABLE#{' IF NOT EXISTS' if options[:if_not_exists]} #{quote_schema_table(name)}"
      end

      def create_table_sql(name, generator, options)
        sql = super
        sql << create_table_parameters_sql(options)
        sql
      end

      def create_table_parameters_sql(options)
        sql = ""
        sql << " STORED AS #{options[:stored_as]}" if options[:stored_as]
        sql << " LOCATION #{literal(options[:location])}" if options[:location]
        sql
      end

      def identifier_input_method_default
        nil
      end
     
      def identifier_output_method_default
        nil
      end

      def metadata_dataset
        @metadata_dataset ||= (
          ds = dataset;
          ds.identifier_input_method = identifier_input_method_default;
          ds.identifier_output_method = :downcase;
          ds
        )
      end

      def type_literal_generic_float(column)
        :double
      end

      def type_literal_generic_numeric(column)
        column[:size] ? "decimal(#{Array(column[:size]).join(', ')})" : :decimal
      end

      def type_literal_generic_string(column)
        if size = column[:size]
          "#{'var' unless column[:fixed]}char(#{size})"
        else
          :string
        end
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

      def date_add_sql_append(sql, da)
        h = da.interval
        expr = da.expr
        intervals = []
        each_valid_interval_unit(h, Sequel::SQL::DateAdd::DatasetMethods::DEF_DURATION_UNITS) do |value, sql_unit|
          intervals << Sequel.lit("INTERVAL #{value} #{sql_unit}")
        end
        if intervals.empty?
          return literal_append(sql, Sequel.cast(expr, Time))
        else
          intervals.unshift(Sequel.cast(expr, Time))
          return complex_expression_sql_append(sql, :+, intervals)
        end
      end

      def delete
        super
        nil
      end

      def delete_sql
        sql = "INSERT OVERWRITE "
        source_list_append(sql, opts[:from])
        sql << " SELECT * FROM "
        source_list_append(sql, opts[:from])
        if where = opts[:where]
          sql << " WHERE NOT ("
          literal_append(sql, where)
          sql << ")"
        else
          sql << " WHERE false"
        end
        sql
      end

      def truncate_sql
        ds = clone
        ds.opts.delete(:where)
        ds.delete_sql
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

      def to_parquet(table, options=OPTS)
        db.create_table(table, options.merge(:as=>self, :stored_as=>:parquet))
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

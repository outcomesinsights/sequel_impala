require 'sequel/adapters/utils/unmodified_identifiers'

module Sequel
  module Impala
    Sequel::Database.set_shared_adapter_scheme :impala, self

    module DatabaseMethods
      include UnmodifiedIdentifiers::DatabaseMethods

      # Do not use a composite primary key, foreign keys, or an
      # index when creating a join table, as Impala doesn't support those.
      def create_join_table(hash, options=OPTS)
        keys = hash.keys.sort_by(&:to_s)
        create_table(join_table_name(hash, options), options) do
          keys.each do |key|
            Integer key
          end
        end
      end

      def refresh(table_name)
        run(refresh_sql(table_name))
      end

      def compute_stats(table_name)
        run(compute_stats_sql(table_name))
      end

      # Create a database/schema in Imapala.
      #
      # Options:
      # :if_not_exists :: Don't raise an error if the schema already exists.
      # :location :: Set the file system location to store the data for tables
      #              in the created schema.
      #
      # Examples:
      #
      #   create_schema(:s)
      #   # CREATE SCHEMA `s`
      #
      #   create_schema(:s, :if_not_exists=>true)
      #   # CREATE SCHEMA IF NOT EXISTS `s`
      #
      #   create_schema(:s, :location=>'/a/b')
      #   # CREATE SCHEMA `s` LOCATION '/a/b'
      def create_schema(schema, options=OPTS)
        run(create_schema_sql(schema, options))
      end

      def create_table(name, options=OPTS)
        if options[:sort_by] && options[:as] && options[:create_unsorted] != false
          # Use of SORT BY with CREATE TABLE AS on complex queries can
          # cause Impala to fail complaining about sort.columns.
          # Use a temporary table in this case to make sure things are sorted.
          orig_name = name
          unsorted_name = name = case name
          when SQL::QualifiedIdentifier
            SQL::QualifiedIdentifier.new(name.table, "#{name.column}_u")
          when SQL::Identifier
            SQL::Identifier.new("#{name.value}_u")
          when LiteralString
            LiteralString.new("#{name}_u")
          when String, Symbol
            "#{name}_u"
          else
            raise Error, "unsupported create_table name argument: #{name.inspect}"
          end
          options = options.dup
          sort_by = options.delete(:sort_by)
        end

        super

        if sort_by
          options = options.merge(:sort_by=>sort_by, :as=>from(name), :create_unsorted=>false)
          name = orig_name
          super
          drop_table(unsorted_name)
        end

        if im = options[:invalidate_metadata]
          invalidate_metadata((name unless im == :all))
        end
      end

      # Set the database_type for this database to :impala.
      def database_type
        :impala
      end

      # Return the DESCRIBE output for the table, showing table
      # columns, types, and comments.  If the :formatted option
      # is given, use DESCRIBE FORMATTED and return a lot more
      # information about the table.  Both of these return arrays
      # of hashes.
      #
      # Examples:
      #
      #   describe(:t)
      #   # DESCRIBE `t`
      #
      #   describe(:t, :formatted=>true)
      #   # DESCRIBE FORMATTED `t`
      def describe(table, opts=OPTS)
        if ds = opts[:dataset]
          ds = ds.naked
        else
          ds = dataset
        end
        ds.with_sql("DESCRIBE #{'FORMATTED ' if opts[:formatted]} ?", table).all
      end

      # Drop a database/schema from Imapala.
      #
      # Options:
      # :if_exists :: Don't raise an error if the schema doesn't exist.
      #
      # Examples:
      #
      #   drop_schema(:s)
      #   # DROP SCHEMA `s`
      #
      #   create_schema(:s, :if_exists=>true)
      #   # DROP SCHEMA IF EXISTS `s`
      def drop_schema(schema, options=OPTS)
        run(drop_schema_sql(schema, options))
      end

      # Implicitly quailfy the table if using the :search_path option.
      # This will look at all of the tables and views in the schemas,
      # and if an unqualified table is used and appears in one of the
      # schemas, it will be implicitly qualified with the given schema
      # name.
      def implicit_qualify(table)
        return table unless opts[:search_path]

        case table
        when Symbol
          s, t, a = Sequel.split_symbol(table)
          if s
            return table
          end
          t = implicit_qualify(t)
          a ? Sequel.as(t, a) : t
        when String
          if schema = search_path_table_schemas[table]
            Sequel.qualify(schema, table)
          else
            invalidate_table_schemas
            if schema = search_path_table_schemas[table]
              Sequel.qualify(schema, table)
            else
              Sequel.identifier(table)
            end
          end
        when SQL::Identifier
          implicit_qualify(table.value.to_s)
        when SQL::AliasedExpression
          SQL::AliasedExpression.new(implicit_qualify(table.expression), table.alias)
        else
          table
        end
      end

      # Invalidate the metadata for the given table, or for all tables if
      # no argument is given.
      def invalidate_metadata(identifier=nil)
        run("INVALIDATE METADATA #{quote_schema_table(identifier) if identifier}")
      end

      # Load data from HDFS into Impala.
      #
      # Options:
      # :overwrite :: Overwrite the existing table instead of appending to it.
      #
      # Examples:
      #
      #  load_data('/user/foo', :bar)
      #  LOAD DATA INPATH '/user/foo' INTO TABLE `bar`
      #
      #  load_data('/user/foo', :bar, :overwrite=>true)
      #  LOAD DATA INPATH '/user/foo' OVERWRITE INTO TABLE `bar`
      def load_data(path, table, options=OPTS)
        run(load_data_sql(path, table, options))
      end

      # Don't use PRIMARY KEY or AUTOINCREMENT on Impala, as Impala doesn't
      # support either.
      def serial_primary_key_options
        {:type=>Integer}
      end

      # Impala supports CREATE TABLE IF NOT EXISTS.
      def supports_create_table_if_not_exists?
        true
      end

      # Impala does not support foreign keys.
      def supports_foreign_key_parsing?
        false
      end

      # Impala does not support indexes.
      def supports_index_parsing?
        false
      end

      # If :table_exists_uses_show_tables Database option is used,
      # use SHOW TABLES [ IN database_name ] LIKE 'table_name' to
      # determine if the table exists.  This can avoid errors showing
      # up in the Impala query logs.
      def table_exists?(name)
        return super unless opts[:table_exists_uses_show_tables]

        sch, table_name = schema_and_table(name)
        sql = String.new
        sql << "SHOW TABLES"
        sql << " IN #{quote_identifier(sch)}" if sch
        sql << " LIKE #{literal(dataset.escape_like(table_name).downcase)}"
        dataset.with_sql(sql).all.length == 1
      end

      # Check that the tables returned by the JDBC driver are actually valid
      # tables and not views.  The Hive2 JDBC driver returns views when listing
      # tables and nothing when listing views.
      def tables(opts=OPTS)
        _tables(opts).select{|t| is_valid_table?(t, opts)}
      end

      # Impala doesn't support transactions, so instead of issuing a
      # transaction, just checkout a connection.  This ensures the same
      # connection is used for the transaction block, but as Impala
      # doesn't support transactions, you can't rollback.
      def transaction(opts=OPTS)
        synchronize(opts[:server]) do |c|
          yield c
        end
      end

      # Determine the available views for listing all tables via JDBC (which
      # includes both tables and views), and removing all valid tables.
      def views(opts=OPTS)
        _tables(opts).reject{|t| is_valid_table?(t, opts)}
      end

      # Creates a dataset that uses the VALUES clause:
      #
      #   DB.values([[1, 2], [3, 4]])
      #   VALUES ((1, 2), (3, 4))
      def values(v)
        @default_dataset.clone(:values=>v)
      end

      def invalidate_table_schemas
        @search_path_table_schemas = nil
      end

      # Sets options in the current db connection for each key/value pair
      def set(opts)
        set_sql(opts).each do |sql|
          run(sql)
        end
      end

      private

      def _tables(opts)
        tabs = metadata_dataset.with_sql("SHOW TABLES#{" IN #{quote_identifier(opts[:schema])}" if opts[:schema]}#{" LIKE #{literal(opts[:like])}" if opts[:like]}").
          select_map(:name).map(&output_identifier_meth)
        tabs = tabs.map { |tab| Sequel.qualify(opts[:schema], tab) } if opts[:schema] && opts[:qualify]
        tabs
      end

      # Impala uses ADD COLUMNS instead of ADD COLUMN.  As its use of
      # ADD COLUMNS implies, it supports adding multiple columns at once,
      # but this adapter doesn't offer an API for that.
      def alter_table_add_column_sql(table, op)
        "ADD COLUMNS (#{column_definition_sql(op)})"
      end

      # Impala uses CHANGE instead of having separate RENAME syntax
      # for renaming tables.  As CHANGE requires a type, look up the
      # type from the database schema.
      def alter_table_rename_column_sql(table, op)
        old_name = op[:name]
        opts = schema(table).find{|x| x.first == old_name}
        opts = opts ? opts.last : {}
        unless opts[:db_type]
          raise Error, "cannot determine database type to use for CHANGE COLUMN operation"
        end
        new_col = op.merge(:type=>opts[:db_type], :name=>op[:new_name])
        "CHANGE #{quote_identifier(old_name)} #{column_definition_sql(new_col)}"
      end

      def alter_table_set_column_type_sql(table, op)
        "CHANGE #{quote_identifier(op[:name])} #{column_definition_sql(op)}"
      end

      # Add COMMENT when defining the column, if :comment is present.
      def column_definition_comment_sql(sql, column)
        sql << " COMMENT #{literal(column[:comment])}" if column[:comment]
      end

      def column_definition_order
        [:comment]
      end

      def create_schema_sql(schema, options)
        "CREATE SCHEMA #{'IF NOT EXISTS ' if options[:if_not_exists]}#{quote_identifier(schema)}#{" LOCATION #{literal(options[:location])}" if options[:location]}"
      end

      # Support using table parameters for CREATE TABLE AS, necessary for
      # creating parquet files from datasets.
      def create_table_as_sql(name, sql, options)
        "#{create_table_prefix_sql(name, options)}#{create_table_parameters_sql(options) } AS #{sql}"
      end

      def create_table_prefix_sql(name, options)
        "CREATE #{'EXTERNAL ' if options[:external]}TABLE#{' IF NOT EXISTS' if options[:if_not_exists]} #{quote_schema_table(name)}"
      end

      def create_table_sql(name, generator, options)
        sql = super
        sql += create_table_parameters_sql(options)
        sql
      end

      def create_table_parameters_sql(options)
        sql = String.new
        sql << " SORT BY #{literal(options[:sort_by])}" if options[:sort_by]
        sql << " COMMENT #{literal(options[:comment])}" if options[:comment]
        if options[:field_term] || options[:line_term]
          sql << " ROW FORMAT DELIMITED"
          if options[:field_term]
            sql << " FIELDS TERMINATED BY #{literal(options[:field_term])}"
            sql << " ESCAPED BY #{literal(options[:field_escape])}" if options[:field_escape]
          end
          if options[:line_term]
            sql << " LINES TERMINATED BY #{literal(options[:line_term])}"
          end
        end
        sql << " STORED AS #{options[:stored_as]}" if options[:stored_as]
        sql << " LOCATION #{literal(options[:location])}" if options[:location]
        sql
      end

      def refresh_sql(table_name)
        "REFRESH #{quote_schema_table(table_name)}"
      end

      def compute_stats_sql(table_name)
        "COMPUTE STATS #{quote_schema_table(table_name)}"
      end

      def drop_schema_sql(schema, options)
        "DROP SCHEMA #{'IF EXISTS ' if options[:if_exists]}#{quote_identifier(schema)}#{' CASCADE' if options[:cascade]}"
      end

      # Support :purge option for PURGE, and don't support CASCADE.
      def drop_table_sql(name, options)
        "DROP TABLE#{' IF EXISTS' if options[:if_exists]} #{quote_schema_table(name)}#{' PURGE' if options[:purge]}"
      end

      def search_path_table_schemas
        @search_path_table_schemas ||= begin
          search_path = opts[:search_path]
          search_path = search_path.split(',') if search_path.is_a?(String)
          table_schemas = {}
          search_path.reverse_each do |schema|
            _tables(:schema=>schema).each do |table|
              table_schemas[table.to_s] = schema.to_s
            end
          end
          table_schemas
        end
      end

      # Use a separate DESCRIBE query for each table to determine if it is a
      # table or a view.
      def is_valid_table?(t, opts=OPTS)
        return true if self.opts[:treat_views_as_tables]

        t = Sequel.qualify(opts[:schema], t) if opts[:schema] && t.is_a?(Symbol)
        rows = describe(t, :formatted=>true)
        if row = rows.find{|r| r[:name].to_s.strip == 'Table Type:'}
          row[:type].to_s.strip !~ /VIEW/
        end
      rescue Sequel::DatabaseError
        # This can be raised for Hive tables that Impala returns via SHOW TABLES,
        # but which it raises an exception when you try to DESCRIBE them.
        false
      end

      def load_data_sql(path, table, options)
        "LOAD DATA INPATH #{literal(path)}#{' OVERWRITE' if options[:overwrite]} INTO TABLE #{literal(table)}"
      end

      # Metadata queries on JDBC use uppercase keys, so set the identifier
      # output method to downcase so that metadata queries work correctly.
      def _metadata_dataset
        super.with_extend do
          def output_identifier(v)
            v.downcase.to_sym
          end
        end
      end

      # Impala doesn't like the word "integer"
      def type_literal_generic_integer(column)
        :int
      end

      # Impala doesn't like the word "biginteger"
      def type_literal_generic_bignum_symbol(column)
        :bigint
      end

      # Impala doesn't like the word "biginteger"
      def type_literal_generic_bignum(column)
        :bigint
      end

      # Impala doesn't support date columns yet, so use timestamp until date
      # is natively supported.
      def type_literal_generic_date(column)
        :timestamp
      end

      # Impala uses double instead of "double precision" for floating point
      # values.
      def type_literal_generic_float(column)
        :double
      end

      # Impala uses decimal instead of numeric for arbitrary precision
      # numeric values.
      def type_literal_generic_numeric(column)
        column[:size] ? "decimal(#{Array(column[:size]).join(', ')})" : :decimal
      end

      # Use char or varchar if given a size, otherwise use string.
      # Using a size is not recommend, as Impala doesn't implicitly
      # cast string values to char or varchar, and doesn't implicitly
      # cast from different sizes of varchar.
      def type_literal_generic_string(column)
        if size = column[:size]
          "#{'var' unless column[:fixed]}char(#{size})"
        else
          :string
        end
      end

      def set_sql(opts)
        opts.map { |k, v| "SET #{k}=#{v}" }
      end

      def force_database(conn, database)
        if database
          log_connection_execute(conn, "USE #{database}")
        end
        conn
      end
    end

    module DatasetMethods
      include UnmodifiedIdentifiers::DatasetMethods

      BACKTICK = '`'.freeze
      APOS = "'".freeze
      STRING_ESCAPE_RE = /([\\'])/
      STRING_ESCAPE_REPLACE = '\\\\\1'.freeze
      BOOL_TRUE = 'true'.freeze
      BOOL_FALSE = 'false'.freeze
      CONSTANT_LITERAL_MAP = {:CURRENT_TIMESTAMP=>'now()'.freeze}.freeze
      PAREN_OPEN = '('.freeze
      PAREN_CLOSE = ')'.freeze
      SPACE = ' '.freeze
      NOT = 'NOT '.freeze
      REGEXP = ' REGEXP '.freeze
      EXCEPT_SOURCE_COLUMN = :__source__
      EXCEPT_STRATEGIES = [:not_exists, :not_in, :left_join, :group_by].freeze
      SELECT_VALUES = 'VALUES '.freeze

      Dataset.def_sql_method(self, :select, [['if opts[:values]', %w'values'], ['else', %w'with select distinct columns from join where group having compounds order limit']])

      # Handle string concatenation using the concat string function.
      # Don't use the ESCAPE syntax when using LIKE/NOT LIKE, as
      # Impala doesn't support escaping LIKE metacharacters.
      # Support regexps on Impala using the REGEXP operator.
      # For cast insensitive regexps, cast both values to uppercase first.
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

      # Use now() for current timestamp, as Impala doesn't support
      # CURRENT_TIMESTAMP.
      def constant_sql_append(sql, constant)
        sql << CONSTANT_LITERAL_MAP.fetch(constant, constant.to_s)
      end

      # Use the addition operator combined with interval types to
      # handle date arithmetic when using the date_arithmetic
      # extension.
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

      # DELETE is emulated on Impala and doesn't return the number of
      # modified rows.
      def delete
        super
        nil
      end

      # Emulate DELETE using INSERT OVERWRITE selecting all columns from
      # the table, with a reversed condition used for WHERE.
      def delete_sql
        return @opts[:prepared_sql] if @opts[:prepared_sql]
        sql = @opts[:append_sql] || sql_string_origin
        sql << "INSERT OVERWRITE "
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

      # Implicitly qualify tables if using the :search_path database option.
      def from(*)
        ds = super
        ds.clone(:from => ds.opts[:from].map{|t| db.implicit_qualify(t)})
      end

      # Create a LEFT SEMI join
      def left_semi_join(*args, &block)
        join_table(:left_semi, *args, &block)
      end

      # Create a RIGHT SEMI join
      def right_semi_join(*args, &block)
        join_table(:right_semi, *args, &block)
      end

      # Handle the :hints option to specify join hints, such as :broadcast,
      # :shuffle, or an array containing both. Use of a join hint automatically
      # forces the use of the STRAIGHT_JOIN in the query.
      def join_table(type, table, expr=nil, options=OPTS, &block)
        ds = super(type, db.implicit_qualify(table), expr, options, &block)

        if join_hints = options[:hints]
          hints = opts[:join_hints] ? opts[:join_hints].dup : {}
          hints[ds.opts[:join].last] = "/* +#{Array(join_hints).join('|').upcase} */ ".freeze
          hints.freeze
          ds = ds.clone(:join_hints=>hints)
        end

        ds
      end

      # Emulate TRUNCATE by using INSERT OVERWRITE selecting all columns
      # from the table, with WHERE false.
      def truncate_sql
        unfiltered.delete_sql
      end

      # Don't remove an order, because that breaks things when offsets
      # are used, as Impala requires an order when using an offset.
      def empty?
        get(Sequel::SQL::AliasedExpression.new(1, :one)).nil?
      end

      # Emulate EXCEPT using a chosen strategy and checking for values in only the first table.
      def except(other, opts=OPTS)
        raise(InvalidOperation, "EXCEPT ALL not supported") if opts[:all]
        raise(InvalidOperation, "The :from_self=>false option to except is not supported") if opts[:from_self] == false

        strategy, *keys = @opts[:except_strategy]
        ds = from_self(:alias=>:t1)

        ds = case strategy
        when :not_exists
          ds.exclude(other.
              from_self(:alias=>:t2).
              where(keys.map{|key| [Sequel.qualify(:t1, key), Sequel.qualify(:t2, key)]}).
              select(nil).
              exists)
        when :not_in
          raise Sequel::Error, ":not_in EXCEPT strategy only supports a single key" unless keys.length == 1
          key = keys.first
          ds.exclude(Sequel.qualify(:t1, key)=>other.from_self(:alias=>:t2).select(key))
        when :left_join
          ds.left_join(other.from_self(:alias=>:t2).as(:t2), keys.map{|key| [key, key]}).
            where(Sequel.or(keys.map{|key| [Sequel.qualify(:t2, key), nil]})).
            select_all(:t1)
        else
          cols = columns
          rhs = other.from_self.select_group(*other.columns).select_append(Sequel.expr(2).as(EXCEPT_SOURCE_COLUMN))
          ds.select_group(*cols).
            select_append(Sequel.expr(1).as(EXCEPT_SOURCE_COLUMN)).
            union(rhs, all: true).
            select_group(*cols).
            having{{count.function.* => 1, min(EXCEPT_SOURCE_COLUMN) => 1}}
        end

        ds.from_self(opts)
      end

      # The strategy to use for EXCEPT emulation. By default, uses a GROUP BY emulation,
      # as that doesn't require you provide a key column, but you can use this to choose
      # a NOT EXISTS, NOT IN, or LEFT JOIN emulation, providing the unique key column.
      def except_strategy(strategy, *keys)
        raise Sequel::Error, "invalid EXCEPT strategy: #{strategy.inspect}" unless EXCEPT_STRATEGIES.include?(strategy)
        clone(:except_strategy=>[strategy, *keys])
      end

      # Use INSERT OVERWRITE instead of INSERT INTO when inserting into this dataset:
      #
      #   DB[:table].insert_overwrite.insert(DB[:other])
      #   # INSERT OVERWRITE table SELECT * FROM other
      def insert_overwrite
        clone(:insert_overwrite=>true)
      end

      # Impala does not support INSERT DEFAULT VALUES.
      def insert_supports_empty_values?
        false
      end

      # Emulate INTERSECT using a join and checking for values in both tables.
      def intersect(other, opts=OPTS)
        raise(InvalidOperation, "INTERSECT ALL not supported") if opts[:all]
        raise(InvalidOperation, "The :from_self=>false option to intersect is not supported") if opts[:from_self] == false
        raise(Error, "Attempt to INTERSECT on dataset with no columns: #{inspect}") if columns.empty?
        raise(Error, "Attempt to INTERSECT other dataset with no columns: #{other.inspect}") if other.columns.empty?

        cols = columns.zip(other.columns)
        from_self(alias: :l)
          .join(other){|lj, j, _| Sequel.&(*cols.map{|c1,c2| Sequel.expr(Sequel.qualify(lj, c2)=>Sequel.qualify(j, c1)) | {Sequel.qualify(lj, c2)=>nil, Sequel.qualify(j, c1)=>nil}})}
          .select_all(:l)
          .distinct
          .from_self(opts)
      end

      # Impala supports non-recursive common table expressions.
      def supports_cte?(type=:select)
        true
      end

      def supports_cte_in_subqueries?
        true
      end

      # Impala doesn't support derived column lists when aliasing
      # tables.
      def supports_derived_column_lists?
        false
      end

      # Impala doesn't support EXCEPT or INTERSECT, but support is emulated for them.
      # However, EXCEPT ALL and INTERSECT ALL are not emulated.
      def supports_intersect_except_all?
        false
      end

      # Impala only support IS NULL, not IS TRUE or IS FALSE.
      def supports_is_true?
        false
      end

      # Impala doesn't support IN when used with multiple columns.
      def supports_multiple_column_in?
        false
      end

      # Impala supports regexps using the REGEXP operator.
      def supports_regexp?
        true
      end

      # Impala supports window functions.
      def supports_window_functions?
        true
      end

      # Create a parquet file from this dataset.  +table+ should
      # be the table name to create.  To specify a path for the
      # parquet file, use the :location option.
      #
      # Examples:
      #
      #   DB[:t].to_parquet(:p)
      #   # CREATE TABLE `p` STORED AS parquet AS
      #   # SELECT * FROM `t`
      #
      #   DB[:t].to_parquet(:p, :location=>'/a/b')
      #   # CREATE TABLE `p` STORED AS parquet LOCATION '/a/b'
      #   # SELECT * FROM `t`
      def to_parquet(table, options=OPTS)
        db.create_table(table, options.merge(:as=>self, :stored_as=>:parquet))
      end

      # UPDATE is emulated on Impala, and returns nil instead of the number of
      # modified rows
      def update(values=OPTS)
        super
        nil
      end

      # Emulate UPDATE using INSERT OVERWRITE AS SELECT.  For all columns used
      # in the given +values+, use a CASE statement.  In the CASE statement,
      # set the value to the new value if the row matches WHERE conditions of
      # the current dataset, otherwise use the existing value.
      def update_sql(values)
        sql = String.new
        sql << "INSERT OVERWRITE "
        source_list_append(sql, opts[:from])
        sql << " SELECT "
        comma = false

        if where = opts[:where]
          where = Sequel.lit(literal(where))
        else
          where = true
        end

        select_all.columns.each do |c|
          if comma
            sql <<  comma
          else
            comma = ', '
          end

          if values.has_key?(c)
            new_value = values[c]
            literal_append(sql, Sequel.case({where=>new_value}, c).as(c))
          else
            quote_identifier_append(sql, c)
          end
        end
        sql << " FROM "
        source_list_append(sql, opts[:from])
        sql
      end

      def with(name, dataset, opts={})
        if has_cte?(dataset)
          s, ds = hoist_cte(dataset)
          s.with(name, ds, opts)
        else
          super
        end
      end

      def with_recursive(name, nonrecursive, recursive, opts={})
        if has_cte?(nonrecursive)
          s, ds = hoist_cte(nonrecursive)
          s.with_recursive(name, ds, recursive, opts)
        elsif has_cte?(recursive)
          s, ds = hoist_cte(recursive)
          s.with_recursive(name, nonrecursive, ds, opts)
        else
          super
        end
      end

      # Append literalization of JOIN clause without ON or USING to SQL string.
      def join_clause_sql_append(sql, jc)
        table = jc.table
        table_alias = jc.table_alias
        table_alias = nil if table == table_alias && !jc.column_aliases
        sql << ' ' << join_type_sql(jc.join_type) << ' '
        if (hints = opts[:join_hints]) && (hint = hints[jc])
          sql << hint
        end
        identifier_append(sql, table)
        as_sql_append(sql, table_alias, jc.column_aliases) if table_alias
      end

      protected

      # Add the dataset to the list of compounds
      def compound_clone(type, dataset, opts)
        if has_cte?(dataset)
          s, ds = hoist_cte(dataset)
          s.compound_clone(type, ds, opts)
        else
          super
        end
      end

      private

      def has_cte?(ds)
        ds.is_a?(Dataset) && ds.opts[:with]
      end

      # Impala doesn't handle the DEFAULT keyword used in inserts, as all default
      # values in Impala are NULL, so just use a NULL value.
      def insert_empty_columns_values
        [[columns.last], [nil]]
      end

      def literal_true
        BOOL_TRUE
      end

      def literal_false
        BOOL_FALSE
      end

      def insert_into_sql(sql)
        sql << (@opts[:insert_overwrite] ? ' OVERWRITE ' : ' INTO ')
        identifier_append(sql, unaliased_identifier(@opts[:from].first))
      end

      # Double backslashes in all strings, and escape all apostrophes with
      # backslashes.
      def literal_string_append(sql, s)
        sql << APOS << s.to_s.gsub(STRING_ESCAPE_RE, STRING_ESCAPE_REPLACE) << APOS
      end

      def multi_insert_sql_strategy
        :values
      end

      # Impala doesn't support esacping of identifiers, so you can't use backtick in
      # an identifier name.
      def quoted_identifier_append(sql, name)
        sql << BACKTICK << name.to_s << BACKTICK
      end

      def select_distinct_sql(sql)
        super
        sql << " STRAIGHT_JOIN" if opts[:join_hints]
      end

      # Don't include a LIMIT clause if there is no FROM clause.  In general,
      # such queries can only return 1 row.
      def select_limit_sql(sql)
        return unless opts[:from]
        super
      end


      # Support VALUES clause instead of the SELECT clause to return rows.
      def select_values_sql(sql)
        sql << SELECT_VALUES
        expression_list_append(sql, opts[:values])
      end
    end
  end
end

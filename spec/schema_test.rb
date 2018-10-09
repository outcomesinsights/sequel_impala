require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Database schema parser" do
  after do
    DB.drop_table?(:items)
  end

  describe "with identifier mangling" do
    before do
      @iom = DB.identifier_output_method
      @iim = DB.identifier_input_method
      @qi = DB.quote_identifiers?
    end
    after do
      DB.identifier_output_method = @iom
      DB.identifier_input_method = @iim
      DB.quote_identifiers = @qi
    end

    it "should handle a database with a identifier methods" do
      DB.identifier_output_method = :reverse
      DB.identifier_input_method = :reverse
      DB.quote_identifiers = true
      DB.create_table!(:items){Integer :number}
      begin
        DB.schema(:items, :reload=>true).must_be_kind_of(Array)
        DB.schema(:items, :reload=>true).first.first.must_equal :number
      ensure
      end
    end

    it "should handle a dataset with identifier methods different than the database's" do
      DB.identifier_output_method = :reverse
      DB.identifier_input_method = :reverse
      DB.quote_identifiers = true
      DB.create_table!(:items){Integer :number}
      DB.identifier_output_method = @iom
      DB.identifier_input_method = @iim
      ds = DB[:items].
        with_identifier_output_method(:reverse).
        with_identifier_input_method(:reverse)
      begin
        DB.schema(ds, :reload=>true).must_be_kind_of(Array)
        DB.schema(ds, :reload=>true).first.first.must_equal :number
      ensure
        DB.identifier_output_method = :reverse
        DB.identifier_input_method = :reverse
        DB.drop_table(:items)
      end
    end
  end if IDENTIFIER_MANGLING && !DB.frozen?

  it "should not issue an sql query if the schema has been loaded unless :reload is true" do
    DB.create_table!(:items){Integer :number}
    DB.schema(:items, :reload=>true)
    DB.schema(:items)
    DB.schema(:items, :reload=>true)
  end

  it "Model schema should include columns in the table, even if they aren't selected" do
    DB.create_table!(:items){String :a; Integer :number}
    m = Sequel::Model(DB[:items].select(:a))
    m.columns.must_equal [:a]
    m.db_schema[:number][:type].must_equal :integer
  end

  it "should raise an error when the table doesn't exist" do
    proc{DB.schema(:no_table)}.must_raise(Sequel::Error, Sequel::DatabaseError)
  end

  it "should return the schema correctly" do
    DB.create_table!(:items){Integer :number}
    schema = DB.schema(:items, :reload=>true)
    schema.must_be_kind_of(Array)
    schema.length.must_equal 1
    col = schema.first
    col.must_be_kind_of(Array)
    col.length.must_equal 2
    col.first.must_equal :number
    col_info = col.last
    col_info.must_be_kind_of(Hash)
    col_info[:type].must_equal :integer
    DB.schema(:items)
  end

  it "should make :default nil for a NULL default" do
    DB.create_table!(:items){Integer :number}
    assert_nil(DB.schema(:items).first.last[:default])
  end

  it "should parse types from the schema properly" do
    DB.create_table!(:items){Integer :number}
    DB.schema(:items).first.last[:type].must_equal :integer
    DB.create_table!(:items){Fixnum :number}
    DB.schema(:items).first.last[:type].must_equal :integer
    DB.create_table!(:items){Bignum :number}
    DB.schema(:items).first.last[:type].must_equal :integer
    DB.create_table!(:items){Float :number}
    DB.schema(:items).first.last[:type].must_equal :float
    DB.create_table!(:items){BigDecimal :number, :size=>[11, 2]}
    DB.schema(:items).first.last[:type].must_equal :decimal
    DB.create_table!(:items){Numeric :number, :size=>[12, 0]}
    DB.schema(:items).first.last[:type].must_equal :integer
    DB.create_table!(:items){String :number}
    DB.schema(:items).first.last[:type].must_equal :string
    DB.create_table!(:items){Time :number}
    DB.schema(:items).first.last[:type].must_equal :datetime
    DB.create_table!(:items){DateTime :number}
    DB.schema(:items).first.last[:type].must_equal :datetime
    DB.create_table!(:items){Date :number}
    DB.schema(:items).first.last[:type].must_equal :datetime
    DB.create_table!(:items){TrueClass :number}
    DB.schema(:items).first.last[:type].must_equal :boolean
    DB.create_table!(:items){FalseClass :number}
    DB.schema(:items).first.last[:type].must_equal :boolean
  end

  it "should parse maximum length for string columns" do
    DB.create_table!(:items){String :a, :size=>4}
    DB.schema(:items).first.last[:max_length].must_equal 4
    DB.create_table!(:items){String :a, :fixed=>true, :size=>3}
    DB.schema(:items).first.last[:max_length].must_equal 3
  end
end

describe "Database schema modifiers" do
  before do
    @db = DB
    @ds = @db[:items]
  end
  after do
    @db.opts[:table_exists_uses_show_tables] = false
    @db.drop_table?(:items)
    @db.drop_table?(:items2)
  end

  it "should create tables correctly" do
    cur = @db.get{current_database.function}
    @db.table_exists?(:items).must_equal false
    @db.table_exists?(Sequel.qualify(cur, :items)).must_equal false

    @db.opts[:table_exists_uses_show_tables] = true
    @db.table_exists?(:items).must_equal false
    @db.table_exists?(Sequel.qualify(cur, :items)).must_equal false

    @db.create_table!(:items){Integer :number}
    @db.opts[:table_exists_uses_show_tables] = false
    @db.table_exists?(:items).must_equal true
    @db.table_exists?(Sequel.qualify(cur, :items)).must_equal true

    @db.opts[:table_exists_uses_show_tables] = true
    @db.table_exists?(:items).must_equal true
    @db.table_exists?(Sequel.qualify(cur, :items)).must_equal true

    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:number]
    @ds.insert([10])
    @ds.columns!.must_equal [:number]
  end

  it "should create tables from select statements correctly" do
    @db.create_table!(:items){Integer :number}
    @ds.insert([10])
    @db.create_table(:items2, :as=>@db[:items])
    @db.schema(:items2, :reload=>true).map{|x| x.first}.must_equal [:number]
    @db[:items2].columns.must_equal [:number]
    @db[:items2].all.must_equal [{:number=>10}]
  end

  it "should not raise an error if table doesn't exist when using drop_table :if_exists" do
    @db.drop_table(:items, :if_exists=>true)
  end

  describe "views" do
    before do
      @db.drop_view(:items_view2) rescue nil
      @db.drop_view(:items_view) rescue nil
      @db.create_table!(:items){Integer :number}
      @ds.insert(:number=>1)
      @ds.insert(:number=>2)
    end
    after do
      @db.drop_view(:items_view2) rescue nil
      @db.drop_view(:items_view) rescue nil
    end

    it "should create views correctly" do
      @db.create_view(:items_view, @ds.where(:number=>1))
      @db[:items_view].map(:number).must_equal [1]
    end

    it "should create views with explicit columns correctly" do
      @db.create_view(:items_view, @ds.where(:number=>1), :columns=>[:n])
      @db[:items_view].map(:n).must_equal [1]
    end

    it "should drop views correctly" do
      @db.create_view(:items_view, @ds.where(:number=>1))
      @db.drop_view(:items_view)
      proc{@db[:items_view].map(:number)}.must_raise(Sequel::DatabaseError)
    end

    it "should not raise an error if view doesn't exist when using drop_view :if_exists" do
      @db.drop_view(:items_view, :if_exists=>true)
    end

    it "should create or replace views correctly" do
      @db.create_or_replace_view(:items_view, @ds.where(:number=>1))
      @db[:items_view].map(:number).must_equal [1]
      @db.create_or_replace_view(:items_view, @ds.where(:number=>2))
      @db[:items_view].map(:number).must_equal [2]
    end
  end

  describe "join tables" do
    after do
      @db.drop_join_table(:cat_id=>:cats, :dog_id=>:dogs) if @db.table_exists?(:cats_dogs)
      @db.drop_table(:cats, :dogs)
      @db.table_exists?(:cats_dogs).must_equal false
    end

    it "should create join tables correctly" do
      @db.create_table!(:cats){primary_key :id}
      @db.create_table!(:dogs){primary_key :id}
      @db.create_join_table(:cat_id=>:cats, :dog_id=>:dogs)
      @db.table_exists?(:cats_dogs).must_equal true
    end
  end

  it "should have create_table? only create the table if it doesn't already exist" do
    @db.create_table!(:items){String :a}
    @db.create_table?(:items){String :b}
    @db[:items].columns.must_equal [:a]
    @db.drop_table?(:items)
    @db.create_table?(:items){String :b}
    @db[:items].columns.must_equal [:b]
  end

  it "should rename tables correctly" do
    @db.drop_table?(:items)
    @db.create_table!(:items2){Integer :number}
    @db.rename_table(:items2, :items)
    @db.table_exists?(:items).must_equal true
    @db.table_exists?(:items2).must_equal false
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:number]
    @ds.insert([10])
    @ds.columns!.must_equal [:number]
  end

  it "should add columns to tables correctly" do
    @db.create_table!(:items){Integer :number}
    @ds.insert(:number=>10)
    @db.alter_table(:items){add_column :name, String}
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:number, :name]
    @ds.columns!.must_equal [:number, :name]
    @ds.all.must_equal [{:number=>10, :name=>nil}]
  end

  it "should rename columns correctly" do
    @db.create_table!(:items){Integer :id}
    @ds.insert(:id=>10)
    @db.alter_table(:items){rename_column :id, :id2}
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:id2]
    @ds.columns!.must_equal [:id2]
    @ds.all.must_equal [{:id2=>10}]
  end

  it "should set column types correctly" do
    @db.create_table!(:items){Integer :id}
    @ds.insert(:id=>10)
    @db.alter_table(:items){set_column_type :id, String}
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:id]
    @ds.columns!.must_equal [:id]
    @ds.insert(:id=>'20')
    @ds.order(:id).all.must_equal [{:id=>"10"}, {:id=>"20"}]
  end

  it "should remove columns from tables correctly" do
    @db.create_table!(:items) do
      primary_key :id
      Integer :i
    end
    @ds.insert(:id=>1, :i=>10)
    @db.drop_column(:items, :i)
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:id]
  end

  it "should remove multiple columns in a single alter_table block" do
    @db.create_table!(:items) do
      primary_key :id
      String :name
      Integer :number
    end
    @ds.insert(:id=>1, :number=>10)
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:id, :name, :number]
    @db.alter_table(:items) do
      drop_column :name
      drop_column :number
    end
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:id]
  end

  it "should work correctly with many operations in a single alter_table call" do
    @db.create_table!(:items) do
      primary_key :id
      String :name2
      String :number2
    end
    @ds.insert(:id=>1, :name2=>'A12')
    @db.alter_table(:items) do
      add_column :number, Integer
      drop_column :number2
      rename_column :name2, :name
    end
    @db[:items].first.must_equal(:id=>1, :name=>'A12', :number=>nil)
  end
end

describe "Database#tables and #views" do
  before do
    class ::String
      @@xxxxx = 0
      def xxxxx
        "xxxxx#{@@xxxxx += 1}"
      end
    end
    @db = DB
    @db.drop_view(:sequel_test_view) rescue nil
    @db.drop_table?(:sequel_test_table)
    @db.create_table(:sequel_test_table){Integer :a}
    @db.create_view :sequel_test_view, @db[:sequel_test_table]
  end
  after do
    @db.opts[:treat_views_as_tables] = false
    @db.drop_view :sequel_test_view
    @db.drop_table :sequel_test_table
  end

  it "#tables should return an array of symbols" do
    ts = @db.tables
    ts.must_be_kind_of(Array)
    ts.each{|t| t.must_be_kind_of(Symbol)}
    ts.must_include(:sequel_test_table)
    ts.wont_include(:sequel_test_view)
  end

  it "#tables should return an array of table and view symbols if :treat_views_as_tables option is used" do
    @db.opts[:treat_views_as_tables] = true
    ts = @db.tables
    ts.must_be_kind_of(Array)
    ts.each{|t| t.must_be_kind_of(Symbol)}
    ts.must_include(:sequel_test_table)
    ts.must_include(:sequel_test_view)
  end

  describe "when using qualify on a known schema" do
    before do
      @db.create_schema(:s1, if_not_exists: true)
      @db.create_table(Sequel.qualify(:s1, :t1)){Integer :a}
    end

    it "#tables should return an array of QualifiedIdentier if qualify: true and schema given" do
      ts = @db.tables(qualify: true, schema: :s1)
      ts.must_be_kind_of(Array)
      ts.each{|t| t.must_be_kind_of(Sequel::SQL::QualifiedIdentifier)}
      ts.must_include(Sequel.qualify(:s1, :t1))
    end

    after do
      @db.drop_schema(:s1, if_exists: true, cascade: true, purge: true)
    end
  end

  it "#views should return an array of symbols" do
    ts = @db.views
    ts.must_be_kind_of(Array)
    ts.each{|t| t.must_be_kind_of(Symbol)}
    ts.wont_include(:sequel_test_table)
    ts.must_include(:sequel_test_view)
  end

  it "#views should return an empty array if :treat_views_as_tables option is used" do
    @db.opts[:treat_views_as_tables] = true
    @db.views.must_equal []
  end

  describe "with identifier mangling" do
    before do
      @iom = @db.identifier_output_method
      @iim = @db.identifier_input_method
    end
    after do
      @db.identifier_output_method = @iom
      @db.identifier_input_method = @iim
    end

    it "#tables should respect the database's identifier_output_method" do
      @db.identifier_output_method = :xxxxx
      @db.identifier_input_method = :xxxxx
      @db.tables.each{|t| t.to_s.must_match(/\Ax{5}\d+\z/)}
    end

    it "#views should respect the database's identifier_output_method" do
      @db.identifier_output_method = :xxxxx
      @db.identifier_input_method = :xxxxx
      @db.views.each{|t| t.to_s.must_match(/\Ax{5}\d+\z/)}
    end
  end if IDENTIFIER_MANGLING && !DB.frozen?
end

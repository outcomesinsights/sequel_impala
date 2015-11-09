require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Impala dataset" do
  before do
    @db = DB
    @db.create_table!(:items) do
      Integer :id
      Integer :number
      String :name
    end
    @ds = @db[:items].order(:id)
    @ds.insert(1, 10, 'a')
  end
  after do
    @db.drop_table?(:items)
  end

  it "#update should emulate UPDATE" do
    @ds.update(:number=>20, :name=>'b')
    @ds.all.must_equal [{:id=>1, :number=>20, :name=>'b'}]
    @ds.where(:id=>1).update(:number=>30, :name=>'c')
    @ds.all.must_equal [{:id=>1, :number=>30, :name=>'c'}]
    @ds.where(:id=>2).update(:number=>40, :name=>'d')
    @ds.all.must_equal [{:id=>1, :number=>30, :name=>'c'}]
  end
end

describe "Impala dataset" do
  before do
    @db = DB
    @db.create_table!(:items){Integer :number}
    @ds = @db[:items]
    @ds.insert(1)
  end
  after do
    @db.drop_table?(:items)
  end

  it "#delete should emulate DELETE" do
    @ds.where(:number=>2).delete.must_equal nil
    @ds.count.must_equal 1
    @ds.where(:number=>1).delete.must_equal nil
    @ds.count.must_equal 0

    @ds.insert(1)
    @ds.count.must_equal 1
    @ds.delete.must_equal nil
    @ds.count.must_equal 0
  end

  it "#truncate should emulate TRUNCATE" do
    @ds.truncate.must_equal nil
    @ds.count.must_equal 0
  end
end

describe "Impala string comparisons" do
  before do
    @db = DB
    @db.create_table!(:items){String :name}
    @ds = @db[:items]
    @ds.insert('m')
  end
  after do
    @db.drop_table?(:items)
  end

  it "should work for equality and inequality" do
    @ds.where(:name => 'm').all.must_equal [{:name=>'m'}]
    @ds.where(:name => 'j').all.must_equal []
    @ds.exclude(:name => 'j').all.must_equal [{:name=>'m'}]
    @ds.exclude(:name => 'm').all.must_equal []
    @ds.where{name > 'l'}.all.must_equal [{:name=>'m'}]
    @ds.where{name > 'm'}.all.must_equal []
    @ds.where{name < 'n'}.all.must_equal [{:name=>'m'}]
    @ds.where{name < 'm'}.all.must_equal []
    @ds.where{name >= 'm'}.all.must_equal [{:name=>'m'}]
    @ds.where{name >= 'n'}.all.must_equal []
    @ds.where{name <= 'm'}.all.must_equal [{:name=>'m'}]
    @ds.where{name <= 'l'}.all.must_equal []
  end
end

describe "Impala date manipulation functions" do
  before do
    @db = DB
    @db.create_table!(:items){Time :t}
    @ds = @db[:items]
    @ds.insert(Date.today)
  end
  after do
    @db.drop_table?(:items)
  end

  it "date_add should work correctly" do
    @ds.get{date_add(t, 0)}.to_date.must_equal Date.today
    @ds.get{date_add(t, Sequel.lit('interval 0 days'))}.to_date.must_equal Date.today
    @ds.get{date_add(t, 1)}.to_date.must_equal(Date.today+1)
    @ds.get{date_add(t, Sequel.lit('interval 1 day'))}.to_date.must_equal(Date.today+1)
    @ds.get{date_add(t, -1)}.to_date.must_equal(Date.today-1)
    @ds.get{date_add(t, Sequel.lit('interval -1 day'))}.to_date.must_equal(Date.today-1)
  end

  it "should work with Sequel date_arithmetic extension" do
    @ds.extension!(:date_arithmetic)
    @ds.get(Sequel.date_add(:t, :days=>1)).to_date.must_equal(Date.today+1)
    @ds.get(Sequel.date_sub(:t, :days=>1)).to_date.must_equal(Date.today-1)
  end
end

describe "Impala create_table options" do
  it "should produce correct sql" do
    DB.send(:create_table_sql, :t, Sequel::Schema::CreateTableGenerator.new(DB){}, :external=>true).must_equal 'CREATE EXTERNAL TABLE `t` ()'
    DB.send(:create_table_sql, :t, Sequel::Schema::CreateTableGenerator.new(DB){}, :stored_as=>:parquet).must_equal 'CREATE TABLE `t` () STORED AS parquet'
    DB.send(:create_table_sql, :t, Sequel::Schema::CreateTableGenerator.new(DB){}, :location=>'/a/b').must_equal "CREATE TABLE `t` () LOCATION '/a/b'"
  end
end

describe "Impala parquet support" do
  before do
    @db = DB
  end
  after do
    @db.drop_table?(:items)
    @db.drop_table?(:items2)
  end

  it "should support parquet format using create_table :stored_as option" do
    @db.create_table!(:items, :stored_as=>:parquet){Integer :number}
    @ds = @db[:items]
    @ds.insert(1)
    @ds.all.must_equal [{:number=>1}]
  end

  it "should support parquet format via Dataset#to_parquet" do
    @db.create_table!(:items){Integer :number}
    @ds = @db[:items]
    @ds.insert(1)
    @ds.to_parquet(:items2)
    @db[:items2].all.must_equal [{:number=>1}]
  end
end

describe "Impala create/drop schemas" do
  it "should use correct SQL" do
    DB.send(:create_schema_sql, :s1, {}).must_equal "CREATE SCHEMA `s1`"
    DB.send(:create_schema_sql, :s1, :if_not_exists=>true).must_equal "CREATE SCHEMA IF NOT EXISTS `s1`"
    DB.send(:create_schema_sql, :s1, :location=>'/a/b').must_equal "CREATE SCHEMA `s1` LOCATION '/a/b'"

    DB.send(:drop_schema_sql, :s1, {}).must_equal "DROP SCHEMA `s1`"
    DB.send(:drop_schema_sql, :s1, :if_exists=>true).must_equal "DROP SCHEMA IF EXISTS `s1`"
  end

  it "should support create_schema and drop_schema" do
    DB.create_schema(:s1)
    DB.create_schema(:s1, :if_not_exists=>true)
    proc{DB.create_schema(:s1)}.must_raise Sequel::DatabaseError
    DB.create_table(:s1__items){Integer :number}
    DB[:s1__items].insert(1)
    DB[:s1__items].all.must_equal [{:number=>1}]
    DB.drop_table(:s1__items)
    DB.drop_schema(:s1)
    proc{DB.drop_schema(:s1)}.must_raise Sequel::DatabaseError
    DB.drop_schema(:s1, :if_exists=>true)
  end
end

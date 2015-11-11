require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Supported types" do
  def create_items_table_with_column(name, type, opts={})
    DB.create_table!(:items){column name, type, opts}
    DB[:items]
  end

  after(:all) do
    DB.drop_table?(:items)
  end

  it "should support casting correctly" do
    ds = create_items_table_with_column(:number, Integer)
    ds.insert(:number => 1)
    ds.select(Sequel.cast(:number, String).as(:n)).map(:n).must_equal %w'1'
    ds = create_items_table_with_column(:name, String)
    ds.insert(:name=> '1')
    ds.select(Sequel.cast(:name, Integer).as(:n)).map(:n).must_equal [1]
  end

  it "should support NULL correctly" do
    ds = create_items_table_with_column(:number, Integer)
    ds.insert(:number => nil)
    ds.all.must_equal [{:number=>nil}]
  end

  it "should support generic integer type" do
    ds = create_items_table_with_column(:number, Integer)
    ds.insert(:number => 2)
    ds.all.must_equal [{:number=>2}]
  end
  
  it "should support generic fixnum type" do
    ds = create_items_table_with_column(:number, Fixnum)
    ds.insert(:number => 2)
    ds.all.must_equal [{:number=>2}]
  end
  
  it "should support generic bignum type" do
    ds = create_items_table_with_column(:number, Bignum)
    ds.insert(:number => 2**34)
    ds.all.must_equal [{:number=>2**34}]
  end
  
  it "should support generic float type" do
    ds = create_items_table_with_column(:number, Float)
    ds.insert(:number => 2.1)
    ds.all.must_equal [{:number=>2.1}]
  end
  
  it "should support generic numeric type" do
    ds = create_items_table_with_column(:number, Numeric, :size=>[15, 10])
    ds.insert(:number => BigDecimal.new('2.123456789'))
    ds.all.must_equal [{:number=>BigDecimal.new('2.123456789')}]
    ds = create_items_table_with_column(:number, BigDecimal, :size=>[15, 10])
    ds.insert(:number => BigDecimal.new('2.123456789'))
    ds.all.must_equal [{:number=>BigDecimal.new('2.123456789')}]
  end

  it "should support generic string type" do
    ds = create_items_table_with_column(:name, String)
    ds.insert(:name => 'Test User')
    ds.all.must_equal [{:name=>'Test User'}]
  end
  
  it "should support generic string type with size" do
    ds = create_items_table_with_column(:name, String, :size=>100)
    ds.insert(:name => Sequel.cast('Test User', 'varchar(100)'))
    ds.all.must_equal [{:name=>'Test User'}]
  end
  
  it "should support generic datetime type" do
    ds = create_items_table_with_column(:tim, DateTime)
    t = DateTime.now
    ds.insert(:tim => t)
    ds.first[:tim].strftime('%Y%m%d%H%M%S').must_equal t.strftime('%Y%m%d%H%M%S')
    ds = create_items_table_with_column(:tim, Time)
    t = Time.now
    ds.insert(:tim => t)
    ds.first[:tim].strftime('%Y%m%d%H%M%S').must_equal t.strftime('%Y%m%d%H%M%S')
  end
  
  it "should support generic date type" do
    ds = create_items_table_with_column(:d, Date)
    t = Date.today
    ds.insert(:d => t)
    ds.first[:d].strftime('%Y%m%d').must_equal t.strftime('%Y%m%d')
  end
  
  it "should support generic boolean type" do
    ds = create_items_table_with_column(:number, TrueClass)
    ds.insert(:number => true)
    ds.all.must_equal [{:number=>true}]
    ds = create_items_table_with_column(:number, FalseClass)
    ds.insert(:number => true)
    ds.all.must_equal [{:number=>true}]
  end
end

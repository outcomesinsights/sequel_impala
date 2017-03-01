require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Simple Dataset operations" do
  before(:all) do
    @db = DB
    @ds = @db[:items]
  end
  after do
    @db.drop_table?(:items)
  end

  it "should support sequential primary keys with a Bignum" do
    @db.create_table!(:items) do
      primary_key :id, :type=>Bignum
      Integer :number
    end
    @ds << {:id=>1, :number=>20}
    @ds << {:id=>2, :number=>30}
    @ds.order(:number).all.must_equal [{:id => 1, :number=>20}, {:id => 2, :number=>30}]
  end

  it "should have insert work correctly with static SQL" do
    @db.create_table!(:items) do
      primary_key :id
      Integer :number
    end
    @db["INSERT INTO #{@ds.literal(:items)} (id, number) VALUES (2, 30)"].insert
    @ds.all.must_equal [{:id => 2, :number=>30}]
  end

  it "should have insert work correctly when inserting a row with all NULL values" do
    @db.create_table!(:items) do
      Integer :id
      Integer :number
    end
    @ds.insert
    @ds.all.must_equal [{:id=>nil, :number=>nil}]
  end

  it "should support iterating over large numbers of records with paged_each" do
    @db.create_table!(:items) do
      Integer :id
      Integer :number
    end
    @ds.import([:id, :number], (1..10).map{|i| [i, i*10]})

    [:offset, :filter].each do |strategy|
      rows = []
      @ds.order(:number).paged_each(:rows_per_fetch=>5, :strategy=>strategy){|row| rows << row}
      rows.must_equal((1..10).map{|i| {:id=>i, :number=>i*10}})

      rows = []
      @ds.order(:number).paged_each(:rows_per_fetch=>3, :strategy=>strategy){|row| rows << row}
      rows.must_equal((1..10).map{|i| {:id=>i, :number=>i*10}})

      rows = []
      @ds.order(:number, :id).paged_each(:rows_per_fetch=>5, :strategy=>strategy){|row| rows << row}
      rows.must_equal((1..10).map{|i| {:id=>i, :number=>i*10}})

      rows = []
      @ds.reverse_order(:number).paged_each(:rows_per_fetch=>5, :strategy=>strategy){|row| rows << row}
      rows.must_equal((1..10).map{|i| {:id=>i, :number=>i*10}}.reverse)

      rows = []
      @ds.order(Sequel.desc(:number), :id).paged_each(:rows_per_fetch=>5, :strategy=>strategy){|row| rows << row}
      rows.must_equal((1..10).map{|i| {:id=>i, :number=>i*10}}.reverse)
    end

    rows = []
    @ds.order(:number).limit(5, 2).paged_each(:rows_per_fetch=>3){|row| rows << row}
    rows.must_equal((3..7).map{|i| {:id=>i, :number=>i*10}})

    rows = []
    @ds.order(Sequel.*(:number, 2)).paged_each(:rows_per_fetch=>5){|row| rows << row}
    rows.must_equal((1..10).map{|i| {:id=>i, :number=>i*10}})

    rows = []
    @ds.order(Sequel.*(:number, 2)).paged_each(:rows_per_fetch=>5, :strategy=>:filter, :filter_values=>proc{|row, _| [row[:number] * 2]}){|row| rows << row}
    rows.must_equal((1..10).map{|i| {:id=>i, :number=>i*10}})

    if RUBY_ENGINE == 'jruby'
      # check retrival with varying fetch sizes
      array = (1..10).to_a
      [1, 2, 5, 10, 20].each do |i|
        @ds.with_fetch_size(i).select_order_map(:id).must_equal array
      end
    end
  end

  it "should fetch correctly with a limit and offset for different combinations of from and join tables" do
    @db.create_table!(:items) do
      Integer :id
      Integer :number
    end
    @ds.insert(:id=>1, :number=>10)
    @db.create_table!(:items2){primary_key :id2; Integer :number2}
    @db[:items2].insert(:id2=>1, :number2=>10)
    @ds.from(:items, :items2).order(:id).limit(2, 0).all.must_equal [{:id=>1, :number=>10, :id2=>1, :number2=>10}]
    @ds.from(:items___i, :items2___i2).order(:id).limit(2, 0).all.must_equal [{:id=>1, :number=>10, :id2=>1, :number2=>10}]
    @ds.cross_join(:items2).order(:id).limit(2, 0).all.must_equal [{:id=>1, :number=>10, :id2=>1, :number2=>10}]
    @ds.from(:items___i).cross_join(:items2___i2).order(:id).limit(2, 0).all.must_equal [{:id=>1, :number=>10, :id2=>1, :number2=>10}]
    @ds.cross_join(:items2___i).cross_join(@db[:items2].select(:id2___id3, :number2___number3)).order(:id).limit(2, 0).all.must_equal [{:id=>1, :number=>10, :id2=>1, :number2=>10, :id3=>1, :number3=>10}]

    @ds.from(:items, :items2).order(:id).limit(2, 1).all.must_equal []
    @ds.from(:items___i, :items2___i2).order(:id).limit(2, 1).all.must_equal []
    @ds.cross_join(:items2).order(:id).limit(2, 1).all.must_equal []
    @ds.from(:items___i).cross_join(:items2___i2).order(:id).limit(2, 1).all.must_equal []
    @ds.cross_join(:items2___i).cross_join(@db[:items2].select(:id2___id3, :number2___number3)).order(:id).limit(2, 1).all.must_equal []
    @db.drop_table(:items2)
  end

end

describe "Simple Dataset operations" do
  before(:all) do
    @db = DB
    @db.create_table!(:items) do
      primary_key :id
      Integer :number
    end
    @ds = @db[:items]
    @ds.insert(:id=>1, :number=>10)
  end
  after(:all) do
    @db.drop_table?(:items)
  end

  it "should join correctly" do
    @ds.join(:items___b, :id=>:id).select_all(:items).all.must_equal [{:id=>1, :number=>10}]
    @ds.join(:items___b, [:id]).select_all(:items).all.must_equal [{:id=>1, :number=>10}]
  end

  it "should correctly handle subqueries" do
    @ds.from_self(:alias=>:a).all.must_equal [{:id=>1, :number=>10}]
    @ds.join(@ds.as(:a), :id=>:id).select_all(:a).all.must_equal [{:id=>1, :number=>10}]
  end

  it "should graph correctly" do
    a =  [{:items=>{:id=>1, :number=>10}, :b=>{:id=>1, :number=>10}}]
    pr = proc{|t| @ds.graph(t, {:id=>:id}, :table_alias=>:b).extension(:graph_each).all.must_equal a}
    pr[:items]
    pr[:items___foo]
    pr[Sequel.identifier(:items)]
    pr[Sequel.identifier('items')]
    pr[Sequel.as(:items, :foo)]
    pr[Sequel.as(Sequel.identifier('items'), 'foo')]
  end

  it "should graph correctly with a subselect" do
    @ds.from_self(:alias=>:items).graph(@ds.from_self, {:id=>:id}, :table_alias=>:b).extension(:graph_each).all.must_equal [{:items=>{:id=>1, :number=>10}, :b=>{:id=>1, :number=>10}}]
  end

  it "should iterate over records as they come in" do
    called = false
    @ds.each{|row| called = true; row.must_equal(:id=>1, :number=>10)}
    called.must_equal true
  end

  it "should fetch all results correctly" do
    @ds.all.must_equal [{:id=>1, :number=>10}]
  end

  it "should fetch a single row correctly" do
    @ds.first.must_equal(:id=>1, :number=>10)
    @ds.single_record.must_equal(:id=>1, :number=>10)
    @ds.single_record!.must_equal(:id=>1, :number=>10)
  end

  it "should work correctly when returning from each without iterating over the whole result set" do
    @ds.order(:id).each{|v| break v}.must_equal(:id=>1, :number=>10)
    @ds.reverse(:id).each{|v| break v}.must_equal(:id=>1, :number=>10)
  end

  it "should fetch a single value correctly" do
    @ds.get(:id).must_equal 1
    @ds.select(:id).single_value.must_equal 1
    @ds.select(:id).single_value!.must_equal 1
  end

  it "should have distinct work with limit" do
    @ds.limit(1).distinct.all.must_equal [{:id=>1, :number=>10}]
  end

  it "should fetch correctly with a limit" do
    @ds.order(:id).limit(1).all.must_equal [{:id=>1, :number=>10}]
  end

  it "should fetch correctly with a limit and offset" do
    @ds.order(:id).limit(1, 0).all.must_equal [{:id=>1, :number=>10}]
    @ds.order(:id).limit(1, 1).all.must_equal []
  end

  it "should fetch correctly with just offset" do
    @ds.order(:id).offset(0).all.must_equal [{:id=>1, :number=>10}]
    @ds.order(:id).offset(1).all.must_equal []
  end

  it "should fetch correctly with a limit and offset using seperate methods" do
    @ds.order(:id).limit(1).offset(0).all.must_equal [{:id=>1, :number=>10}]
    @ds.order(:id).limit(1).offset(1).all.must_equal []
  end

  it "should provide correct columns when using a limit and offset" do
    ds = @ds.order(:id).limit(1, 1)
    ds.all
    ds.columns.must_equal [:id, :number]
    @ds.order(:id).limit(1, 1).columns.must_equal [:id, :number]
  end

  it "should fetch correctly with a limit and offset without an order" do
    ds = @ds.order(1)
    ds.limit(2, 1).all.must_equal []
    ds.join(:items___i, :id=>:id).select(:items__id___s, :i__id___id2).limit(2, 1).all.must_equal []
    ds.join(:items___i, :id=>:id).select(:items__id).limit(2, 1).all.must_equal []
    ds.join(:items___i, :id=>:id).select(Sequel.qualify(:items, :id)).limit(2, 1).all.must_equal []
    ds.join(:items___i, :id=>:id).select(Sequel.qualify(:items, :id).as(:s)).limit(2, 1).all.must_equal []
  end

  it "should be orderable by column number" do
    @ds.order(2, 1).select_map([:id, :number]).must_equal [[1, 10]]
  end

  it "should fetch correctly with a limit in an IN subselect" do
    @ds.where(:id=>@ds.select(:id).order(:id).limit(1)).all.must_equal [{:id=>1, :number=>10}]
  end

  it "should fetch correctly with a limit and offset in an IN subselect" do
    @ds.where(:id=>@ds.select(:id).order(:id).limit(2, 0)).all.must_equal [{:id=>1, :number=>10}]
    @ds.where(:id=>@ds.select(:id).order(:id).limit(2, 1)).all.must_equal []
  end

  it "should fetch correctly when using limit and offset in a from_self" do
    ds = @ds.order(:id).limit(1, 1).from_self
    ds.all.must_equal []
    ds.columns.must_equal [:id, :number]
    @ds.order(:id).limit(1, 1).columns.must_equal [:id, :number]
  end

  it "should fetch correctly when using nested limit and offset in a from_self" do
    ds = @ds.order(:id).limit(1, 0).from_self.reverse_order(:number).limit(1, 0)
    ds.all.must_equal [{:number=>10, :id=>1}]
    ds.columns.must_equal [:id, :number]
    @ds.order(:id).limit(1, 0).from_self.reverse_order(:number).limit(1, 0).columns.must_equal [:id, :number]
  end

  it "should alias columns correctly" do
    @ds.select(:id___x, :number___n).first.must_equal(:x=>1, :n=>10)
  end

  it "should handle true/false properly" do
    @ds.filter(Sequel::TRUE).select_map(:number).must_equal [10]
    @ds.filter(Sequel::FALSE).select_map(:number).must_equal []
    @ds.filter(true).select_map(:number).must_equal [10]
    @ds.filter(false).select_map(:number).must_equal []
    @ds.filter({:id=>1}=>true).select_map(:number).must_equal [10]
    @ds.filter({:id=>1}=>false).select_map(:number).must_equal []
    @ds.literal(true)
    @ds.literal(false)
  end
end

describe Sequel::Dataset do
  before(:all) do
    DB.create_table!(:test) do
      String :name
      Integer :value
    end
    @d = DB[:test]
    @d.multi_insert([{:name => 'abc', :value => 123}, {:name => 'abc', :value => 456}, {:name => 'def', :value => 789}])
  end
  after(:all) do
    DB.drop_table?(:test)
  end

  it "should correctly return avg" do
    @d.avg(:value).to_i.must_equal 456
  end

  it "should correctly return sum" do
    @d.sum(:value).to_i.must_equal 1368
  end

  it "should correctly return max" do
    @d.max(:value).to_i.must_equal 789
  end

  it "should correctly return min" do
    @d.min(:value).to_i.must_equal 123
  end

  it "should return the correct record count" do
    @d.count.must_equal 3
  end

  it "should handle functions with identifier names correctly" do
    @d.get{sum.function(:value)}.must_equal 1368
  end

  it "should handle aggregate methods on limited datasets correctly" do
    @d = @d.order(:name).limit(2)
    @d.count.must_equal 2
    @d.avg(:value).to_i.must_equal 289
    @d.min(:value).to_i.must_equal 123
    @d.reverse(:value).min(:value).to_i.must_equal 456
    @d.max(:value).to_i.must_equal 456
    @d.sum(:value).to_i.must_equal 579
    @d.interval(:value).to_i.must_equal 333
  end

  it "should return the correct records" do
    @d.order(:value).to_a.must_equal [{:name => 'abc', :value => 123}, {:name => 'abc', :value => 456}, {:name => 'def', :value => 789}]
  end
end

describe Sequel::Database do
  it "should correctly escape strings" do
    ["\\\n",
     "\\\\\n",
     "\\\r\n",
     "\\\\\r\n",
     "\\\\\n\n",
     "\\\\\r\n\r\n",
     "\b\a'\0\3",
     #"\t\b\a'\0\3",
     "\\dingo",
     "\\'dingo",
     "\\\\''dingo",
    ].each do |str|
      DB.get(Sequel.cast(str, String)).must_equal str
      str = "1#{str}1"
      DB.get(Sequel.cast(str, String)).must_equal str
      str = "#{str}#{str}"
      DB.get(Sequel.cast(str, String)).must_equal str
    end
  end

  it "should have a working table_exists?" do
    t = :basdfdsafsaddsaf
    DB.drop_table?(t)
    DB.table_exists?(t).must_equal false
    DB.create_table(t){Integer :a}
    begin
      DB.table_exists?(t).must_equal true
    ensure
      DB.drop_table(t)
    end
  end
end

describe "Simple Dataset operations" do
  before do
    DB.create_table!(:items) do
      Integer :number
      TrueClass :flag
    end
    @ds = DB[:items]
  end
  after do
    DB.drop_table?(:items)
  end

  it "should deal with boolean conditions correctly" do
    @ds.insert(:number=>1, :flag=>true)
    @ds.insert(:number=>2, :flag=>false)
    @ds.insert(:number=>3, :flag=>nil)
    @ds.order!(:number)
    @ds.filter(:flag=>true).map(:number).must_equal [1]
    @ds.filter(:flag=>false).map(:number).must_equal [2]
    @ds.filter(:flag=>nil).map(:number).must_equal [3]
    @ds.exclude(:flag=>true).map(:number).must_equal [2, 3]
    @ds.exclude(:flag=>false).map(:number).must_equal [1, 3]
    @ds.exclude(:flag=>nil).map(:number).must_equal [1, 2]
  end
end

describe "Dataset UNION, EXCEPT, and INTERSECT" do
  before(:all) do
    DB.create_table!(:i1){integer :number}
    DB.create_table!(:i2){integer :number}
    DB.create_table!(:i3){integer :number}
    @ds1 = DB[:i1]
    @ds1.insert(:number=>8)
    @ds1.insert(:number=>10)
    @ds1.insert(:number=>20)
    @ds1.insert(:number=>38)
    @ds2 = DB[:i2]
    @ds2.insert(:number=>9)
    @ds2.insert(:number=>10)
    @ds2.insert(:number=>30)
    @ds2.insert(:number=>39)
    @ds3 = DB[:i3]
    @ds3.insert(:number=>10)
    @ds3.insert(:number=>40)
  end
  after(:all) do
    DB.drop_table?(:i1, :i2, :i3)
  end

  it "should give the correct results for simple UNION" do
    @ds1.union(@ds2).order(:number).map{|x| x[:number].to_s}.must_equal %w'8 9 10 20 30 38 39'
  end

  it "should give the correct results for UNION when used with ordering and limits" do

    @ds1.reverse_order(:number).union(@ds2).order(:number).map{|x| x[:number].to_s}.must_equal %w'8 9 10 20 30 38 39'
    @ds1.union(@ds2.reverse_order(:number)).order(:number).map{|x| x[:number].to_s}.must_equal %w'8 9 10 20 30 38 39'

    @ds1.reverse_order(:number).limit(1).union(@ds2).order(:number).map{|x| x[:number].to_s}.must_equal %w'9 10 30 38 39'
    @ds2.reverse_order(:number).limit(1).union(@ds1).order(:number).map{|x| x[:number].to_s}.must_equal %w'8 10 20 38 39'

    @ds1.union(@ds2.order(:number).limit(1)).order(:number).map{|x| x[:number].to_s}.must_equal %w'8 9 10 20 38'
    @ds2.union(@ds1.order(:number).limit(1)).order(:number).map{|x| x[:number].to_s}.must_equal %w'8 9 10 30 39'

    @ds1.union(@ds2).limit(2).order(:number).map{|x| x[:number].to_s}.must_equal %w'8 9'
    @ds2.union(@ds1).reverse_order(:number).limit(2).map{|x| x[:number].to_s}.must_equal %w'39 38'

    @ds1.reverse_order(:number).limit(2).union(@ds2.reverse_order(:number).limit(2)).order(:number).limit(3).map{|x| x[:number].to_s}.must_equal %w'20 30 38'
    @ds2.order(:number).limit(2).union(@ds1.order(:number).limit(2)).reverse_order(:number).limit(3).map{|x| x[:number].to_s}.must_equal %w'10 9 8'
  end

  it "should give the correct results for compound UNION" do
    @ds1.union(@ds2).union(@ds3).order(:number).map{|x| x[:number].to_s}.must_equal %w'8 9 10 20 30 38 39 40'
    @ds1.union(@ds2.union(@ds3)).order(:number).map{|x| x[:number].to_s}.must_equal %w'8 9 10 20 30 38 39 40'
  end
end

describe "Common Table Expressions" do
  before(:all) do
    @db = DB
    @db.create_table!(:i1){Integer :id; Integer :parent_id}
    @ds = @db[:i1]
    @ds.insert(:id=>1)
    @ds.insert(:id=>2)
    @ds.insert(:id=>3, :parent_id=>1)
    @ds.insert(:id=>4, :parent_id=>1)
    @ds.insert(:id=>5, :parent_id=>3)
    @ds.insert(:id=>6, :parent_id=>5)
  end
  after(:all) do
    @db.drop_table?(:i1)
  end

  it "should give correct results for WITH" do
    @db[:t].with(:t, @ds.filter(:parent_id=>nil).select(:id)).order(:id).map(:id).must_equal [1, 2]
  end

  it "should support joining a dataset with a CTE" do
    @ds.inner_join(@db[:t].with(:t, @ds.filter(:parent_id=>nil)), :id => :id).select(:i1__id).order(:i1__id).map(:id).must_equal [1,2]
    @db[:t].with(:t, @ds).inner_join(@db[:s].with(:s, @ds.filter(:parent_id=>nil)), :id => :id).select(:t__id).order(:t__id).map(:id).must_equal [1,2]
  end

  it "should support a subselect in the FROM clause with a CTE" do
    @ds.from(@db[:t].with(:t, @ds)).select_order_map(:id).must_equal [1,2,3,4,5,6]
    @db[:t].with(:t, @ds).from_self.select_order_map(:id).must_equal [1,2,3,4,5,6]
  end

  it "should support using a CTE inside a CTE" do
    @db[:s].with(:s, @db[:t].with(:t, @ds)).select_order_map(:id).must_equal [1,2,3,4,5,6]
    @db[:s].with_recursive(:s, @db[:t].with(:t, @ds), @db[:t2].with(:t2, @ds)).select_order_map(:id).must_equal [1,1,2,2,3,3,4,4,5,5,6,6]
  end

  it "should support using a CTE inside UNION/EXCEPT/INTERSECT" do
    @ds.union(@db[:t].with(:t, @ds)).select_order_map(:id).must_equal [1,2,3,4,5,6]
    if @ds.supports_intersect_except?
      @ds.intersect(@db[:t].with(:t, @ds)).select_order_map(:id).must_equal [1,2,3,4,5,6]
      @ds.except(@db[:t].with(:t, @ds)).select_order_map(:id).must_equal []
    end
  end
end

describe "Window Functions" do
  before(:all) do
    @db = DB
    @db.create_table!(:i1){Integer :id; Integer :group_id; Integer :amount}
    @ds = @db[:i1].order(:id)
    @ds.insert(:id=>1, :group_id=>1, :amount=>1)
    @ds.insert(:id=>2, :group_id=>1, :amount=>10)
    @ds.insert(:id=>3, :group_id=>1, :amount=>100)
    @ds.insert(:id=>4, :group_id=>2, :amount=>1000)
    @ds.insert(:id=>5, :group_id=>2, :amount=>10000)
    @ds.insert(:id=>6, :group_id=>2, :amount=>100000)
  end
  after(:all) do
    @db.drop_table?(:i1)
  end

  it "should give correct results for aggregate window functions" do
    @ds.select(:id){sum(:amount).over(:partition=>:group_id).as(:sum)}.all.
      must_equal [{:sum=>111, :id=>1}, {:sum=>111, :id=>2}, {:sum=>111, :id=>3}, {:sum=>111000, :id=>4}, {:sum=>111000, :id=>5}, {:sum=>111000, :id=>6}]
    @ds.select(:id){sum(:amount).over.as(:sum)}.all.
      must_equal [{:sum=>111111, :id=>1}, {:sum=>111111, :id=>2}, {:sum=>111111, :id=>3}, {:sum=>111111, :id=>4}, {:sum=>111111, :id=>5}, {:sum=>111111, :id=>6}]
  end

  it "should give correct results for ranking window functions with orders" do
    @ds.select(:id){rank{}.over(:partition=>:group_id, :order=>:id).as(:rank)}.all.
      must_equal [{:rank=>1, :id=>1}, {:rank=>2, :id=>2}, {:rank=>3, :id=>3}, {:rank=>1, :id=>4}, {:rank=>2, :id=>5}, {:rank=>3, :id=>6}]
    @ds.select(:id){rank{}.over(:order=>id).as(:rank)}.all.
      must_equal [{:rank=>1, :id=>1}, {:rank=>2, :id=>2}, {:rank=>3, :id=>3}, {:rank=>4, :id=>4}, {:rank=>5, :id=>5}, {:rank=>6, :id=>6}]
  end

  it "should give correct results for aggregate window functions with orders" do
    @ds.select(:id){sum(:amount).over(:partition=>:group_id, :order=>:id).as(:sum)}.all.
      must_equal [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1000, :id=>4}, {:sum=>11000, :id=>5}, {:sum=>111000, :id=>6}]
    @ds.select(:id){sum(:amount).over(:order=>:id).as(:sum)}.all.
      must_equal [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1111, :id=>4}, {:sum=>11111, :id=>5}, {:sum=>111111, :id=>6}]
  end

  it "should give correct results for aggregate window functions with frames" do
    @ds.select(:id){sum(:amount).over(:partition=>:group_id, :order=>:id, :frame=>:all).as(:sum)}.all.
      must_equal [{:sum=>111, :id=>1}, {:sum=>111, :id=>2}, {:sum=>111, :id=>3}, {:sum=>111000, :id=>4}, {:sum=>111000, :id=>5}, {:sum=>111000, :id=>6}]
    @ds.select(:id){sum(:amount).over(:order=>:id, :frame=>:all).as(:sum)}.all.
      must_equal [{:sum=>111111, :id=>1}, {:sum=>111111, :id=>2}, {:sum=>111111, :id=>3}, {:sum=>111111, :id=>4}, {:sum=>111111, :id=>5}, {:sum=>111111, :id=>6}]

    @ds.select(:id){sum(:amount).over(:partition=>:group_id, :order=>:id, :frame=>:rows).as(:sum)}.all.
      must_equal [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1000, :id=>4}, {:sum=>11000, :id=>5}, {:sum=>111000, :id=>6}]
    @ds.select(:id){sum(:amount).over(:order=>:id, :frame=>:rows).as(:sum)}.all.
      must_equal [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1111, :id=>4}, {:sum=>11111, :id=>5}, {:sum=>111111, :id=>6}]
  end
end

describe Sequel::SQL::Constants do
  before do
    @db = DB
    @ds = @db[:constants]
    @c = proc do |v|
      case v
      when Time
        v
      when DateTime, String
        Time.parse(v.to_s)
      else
        v
      end
    end
    @c2 = proc{|v| v.is_a?(Date) ? v : Date.parse(v) }
  end
  after do
    @db.drop_table?(:constants)
  end

  it "should have working CURRENT_TIMESTAMP" do
    @db.create_table!(:constants){DateTime :ts}
    @ds.insert(:ts=>Sequel::CURRENT_TIMESTAMP)
    (Time.now - @c[@ds.get(:ts)]).must_be_close_to 0, 86400
  end
end

describe "Sequel::Dataset#import and #multi_insert" do
  before do
    @db = DB
    @db.create_table!(:imp){Integer :i}
    @ids = @db[:imp].order(:i)
  end
  after do
    @db.drop_table?(:imp)
  end

  it "should import with multi_insert and an array of hashes" do
    @ids.multi_insert([{:i=>10}, {:i=>20}])
    @ids.all.must_equal [{:i=>10}, {:i=>20}]
  end

  it "should import with an array of arrays of values" do
    @ids.import([:i], [[10], [20]])
    @ids.all.must_equal [{:i=>10}, {:i=>20}]
  end

  it "should import with a dataset" do
    @db.create_table!(:exp2){Integer :i}
    @db[:exp2].import([:i], [[10], [20]])
    @ids.import([:i], @db[:exp2])
    @ids.all.must_equal [{:i=>10}, {:i=>20}]
    @db.drop_table(:exp2)
  end

  it "should have import work with the :slice_size option" do
    @ids.import([:i], [[10], [20], [30]], :slice_size=>1)
    @ids.all.must_equal [{:i=>10}, {:i=>20}, {:i=>30}]
  end

  it "should import many rows at once" do
    @ids.import([:i], (1..20).to_a.map{|x| [x]})
    @ids.select_order_map(:i).must_equal((1..20).to_a)
  end
end

describe "Sequel::Dataset convenience methods" do
  before do
    @db = DB
    @db.create_table!(:a){Integer :a; Integer :b}
    @ds = @db[:a].order(:a)
  end
  after do
    @db.drop_table?(:a)
  end

  it "#empty? should return whether the dataset returns no rows" do
    @ds.empty?.must_equal true
    @ds.insert(20, 10)
    @ds.empty?.must_equal false
  end

  it "#empty? should work correctly for datasets with limits" do
    ds = @ds.limit(1)
    ds.empty?.must_equal true
    ds.insert(20, 10)
    ds.empty?.must_equal false
  end

  it "#empty? should work correctly for datasets with limits and offsets" do
    ds = @ds.order(:a).limit(1, 1)
    ds.empty?.must_equal true
    ds.insert(20, 10)
    ds.empty?.must_equal true
    ds.insert(20, 10)
    ds.empty?.must_equal false
  end

  it "#group_and_count should return a grouping by count" do
    @ds.group_and_count(:a).order{count(:a)}.all.must_equal []
    @ds.insert(20, 10)
    @ds.group_and_count(:a).order{count(:a)}.all.each{|h| h[:count] = h[:count].to_i}.must_equal [{:a=>20, :count=>1}]
    @ds.insert(20, 30)
    @ds.group_and_count(:a).order{count(:a)}.all.each{|h| h[:count] = h[:count].to_i}.must_equal [{:a=>20, :count=>2}]
    @ds.insert(30, 30)
    @ds.group_and_count(:a).order{count(:a)}.all.each{|h| h[:count] = h[:count].to_i}.must_equal [{:a=>30, :count=>1}, {:a=>20, :count=>2}]
  end

  it "#group_and_count should support column aliases" do
    @ds.group_and_count(:a___c).order{count(:a)}.all.must_equal []
    @ds.insert(20, 10)
    @ds.group_and_count(:a___c).order{count(:a)}.all.each{|h| h[:count] = h[:count].to_i}.must_equal [{:c=>20, :count=>1}]
    @ds.insert(20, 30)
    @ds.group_and_count(:a___c).order{count(:a)}.all.each{|h| h[:count] = h[:count].to_i}.must_equal [{:c=>20, :count=>2}]
    @ds.insert(30, 30)
    @ds.group_and_count(:a___c).order{count(:a)}.all.each{|h| h[:count] = h[:count].to_i}.must_equal [{:c=>30, :count=>1}, {:c=>20, :count=>2}]
  end

  it "#range should return the range between the maximum and minimum values" do
    @ds = @ds.unordered
    @ds.insert(20, 10)
    @ds.insert(30, 10)
    @ds.range(:a).must_equal(20..30)
    @ds.range(:b).must_equal(10..10)
  end

  it "#interval should return the different between the maximum and minimum values" do
    @ds = @ds.unordered
    @ds.insert(20, 10)
    @ds.insert(30, 10)
    @ds.interval(:a).to_i.must_equal 10
    @ds.interval(:b).to_i.must_equal 0
  end
end

describe "Sequel::Dataset main SQL methods" do
  before do
    @db = DB
    @db.create_table!(:d){Integer :a; Integer :b}
    @ds = @db[:d].order(:a)
  end
  after do
    @db.drop_table?(:d)
  end

  it "#exists should return a usable exists clause" do
    @ds.filter(@db[:d___c].filter(:c__a=>:d__b).exists).all.must_equal []
    @ds.insert(20, 30)
    @ds.insert(10, 20)
    @ds.filter(@db[:d___c].filter(:c__a=>:d__b).exists).all.must_equal [{:a=>10, :b=>20}]
  end

  it "#filter and #exclude should work with placeholder strings" do
    @ds.insert(20, 30)
    @ds.filter("a > ?", 15).all.must_equal [{:a=>20, :b=>30}]
    @ds.exclude("b < ?", 15).all.must_equal [{:a=>20, :b=>30}]
    @ds.filter("b < ?", 15).invert.all.must_equal [{:a=>20, :b=>30}]
  end

  it "#and and #or should work correctly" do
    @ds.insert(20, 30)
    @ds.filter(:a=>20).and(:b=>30).all.must_equal [{:a=>20, :b=>30}]
    @ds.filter(:a=>20).and(:b=>15).all.must_equal []
    @ds.filter(:a=>20).or(:b=>15).all.must_equal [{:a=>20, :b=>30}]
    @ds.filter(:a=>10).or(:b=>15).all.must_equal []
  end

  it "#select_group should work correctly" do
    @ds.unordered!
    @ds.select_group(:a).all.must_equal []
    @ds.insert(20, 30)
    @ds.select_group(:a).all.must_equal [{:a=>20}]
    @ds.select_group(:b).all.must_equal [{:b=>30}]
    @ds.insert(20, 40)
    @ds.select_group(:a).all.must_equal [{:a=>20}]
    @ds.order(:b).select_group(:b).all.must_equal [{:b=>30}, {:b=>40}]
  end

  it "#select_group should work correctly when aliasing" do
    @ds.unordered!
    @ds.insert(20, 30)
    @ds.select_group(:b___c).all.must_equal [{:c=>30}]
  end

  it "#having should work correctly" do
    @ds.unordered!
    @ds.select{[b, max(a).as(c)]}.group(:b).having{max(a) > 30}.all.must_equal []
    @ds.insert(20, 30)
    @ds.select{[b, max(a).as(c)]}.group(:b).having{max(a) > 30}.all.must_equal []
    @ds.insert(40, 20)
    @ds.select{[b, max(a).as(c)]}.group(:b).having{max(a) > 30}.all.each{|h| h[:c] = h[:c].to_i}.must_equal [{:b=>20, :c=>40}]
  end

  it "#having should work without a previous group" do
    @ds.unordered!
    @ds.select{max(a).as(c)}.having{max(a) > 30}.all.must_equal []
    @ds.insert(20, 30)
    @ds.select{max(a).as(c)}.having{max(a) > 30}.all.must_equal []
    @ds.insert(40, 20)
    @ds.select{max(a).as(c)}.having{max(a) > 30}.all.each{|h| h[:c] = h[:c].to_i}.must_equal [{:c=>40}]
  end
end

describe "Sequel::Dataset convenience methods" do
  before do
    @db = DB
    @db.create_table!(:a){Integer :a; Integer :b; Integer :c; Integer :d}
    @ds = @db[:a].order(:a)
    @ds.insert(1, 2, 3, 4)
    @ds.insert(5, 6, 7, 8)
  end
  after do
    @db.drop_table?(:a)
  end

  it "should have working #map" do
    @ds.map(:a).must_equal [1, 5]
    @ds.map(:b).must_equal [2, 6]
    @ds.map([:a, :b]).must_equal [[1, 2], [5, 6]]
  end

  it "should have working #to_hash" do
    @ds.to_hash(:a).must_equal(1=>{:a=>1, :b=>2, :c=>3, :d=>4}, 5=>{:a=>5, :b=>6, :c=>7, :d=>8})
    @ds.to_hash(:b).must_equal(2=>{:a=>1, :b=>2, :c=>3, :d=>4}, 6=>{:a=>5, :b=>6, :c=>7, :d=>8})
    @ds.to_hash([:a, :b]).must_equal([1, 2]=>{:a=>1, :b=>2, :c=>3, :d=>4}, [5, 6]=>{:a=>5, :b=>6, :c=>7, :d=>8})

    @ds.to_hash(:a, :b).must_equal(1=>2, 5=>6)
    @ds.to_hash([:a, :c], :b).must_equal([1, 3]=>2, [5, 7]=>6)
    @ds.to_hash(:a, [:b, :c]).must_equal(1=>[2, 3], 5=>[6, 7])
    @ds.to_hash([:a, :c], [:b, :d]).must_equal([1, 3]=>[2, 4], [5, 7]=>[6, 8])
  end

  it "should have working #to_hash_groups" do
    ds = @ds.order(*@ds.columns)
    ds.insert(1, 2, 3, 9)
    ds.to_hash_groups(:a).must_equal(1=>[{:a=>1, :b=>2, :c=>3, :d=>4}, {:a=>1, :b=>2, :c=>3, :d=>9}], 5=>[{:a=>5, :b=>6, :c=>7, :d=>8}])
    ds.to_hash_groups(:b).must_equal(2=>[{:a=>1, :b=>2, :c=>3, :d=>4}, {:a=>1, :b=>2, :c=>3, :d=>9}], 6=>[{:a=>5, :b=>6, :c=>7, :d=>8}])
    ds.to_hash_groups([:a, :b]).must_equal([1, 2]=>[{:a=>1, :b=>2, :c=>3, :d=>4}, {:a=>1, :b=>2, :c=>3, :d=>9}], [5, 6]=>[{:a=>5, :b=>6, :c=>7, :d=>8}])

    ds.to_hash_groups(:a, :d).must_equal(1=>[4, 9], 5=>[8])
    ds.to_hash_groups([:a, :c], :d).must_equal([1, 3]=>[4, 9], [5, 7]=>[8])
    ds.to_hash_groups(:a, [:b, :d]).must_equal(1=>[[2, 4], [2, 9]], 5=>[[6, 8]])
    ds.to_hash_groups([:a, :c], [:b, :d]).must_equal([1, 3]=>[[2, 4], [2, 9]], [5, 7]=>[[6, 8]])
  end

  it "should have working #select_map" do
    @ds.select_map(:a).must_equal [1, 5]
    @ds.select_map(:b).must_equal [2, 6]
    @ds.select_map([:a]).must_equal [[1], [5]]
    @ds.select_map([:a, :b]).must_equal [[1, 2], [5, 6]]

    @ds.select_map(:a___e).must_equal [1, 5]
    @ds.select_map(:b___e).must_equal [2, 6]
    @ds.select_map([:a___e, :b___f]).must_equal [[1, 2], [5, 6]]
    @ds.select_map([:a__a___e, :a__b___f]).must_equal [[1, 2], [5, 6]]
    @ds.select_map([Sequel.expr(:a__a).as(:e), Sequel.expr(:a__b).as(:f)]).must_equal [[1, 2], [5, 6]]
    @ds.select_map([Sequel.qualify(:a, :a).as(:e), Sequel.qualify(:a, :b).as(:f)]).must_equal [[1, 2], [5, 6]]
    @ds.select_map([Sequel.identifier(:a).qualify(:a).as(:e), Sequel.qualify(:a, :b).as(:f)]).must_equal [[1, 2], [5, 6]]
  end

  it "should have working #select_order_map" do
    @ds.select_order_map(:a).must_equal [1, 5]
    @ds.select_order_map(Sequel.desc(:a__b)).must_equal [6, 2]
    @ds.select_order_map(Sequel.desc(:a__b___e)).must_equal [6, 2]
    @ds.select_order_map(Sequel.qualify(:a, :b).as(:e)).must_equal [2, 6]
    @ds.select_order_map([:a]).must_equal [[1], [5]]
    @ds.select_order_map([Sequel.desc(:a), :b]).must_equal [[5, 6], [1, 2]]

    @ds.select_order_map(:a___e).must_equal [1, 5]
    @ds.select_order_map(:b___e).must_equal [2, 6]
    @ds.select_order_map([Sequel.desc(:a___e), :b___f]).must_equal [[5, 6], [1, 2]]
    @ds.select_order_map([Sequel.desc(:a__a___e), :a__b___f]).must_equal [[5, 6], [1, 2]]
    @ds.select_order_map([Sequel.desc(:a__a), Sequel.expr(:a__b).as(:f)]).must_equal [[5, 6], [1, 2]]
    @ds.select_order_map([Sequel.qualify(:a, :a).desc, Sequel.qualify(:a, :b).as(:f)]).must_equal [[5, 6], [1, 2]]
    @ds.select_order_map([Sequel.identifier(:a).qualify(:a).desc, Sequel.qualify(:a, :b).as(:f)]).must_equal [[5, 6], [1, 2]]
  end

  it "should have working #select_hash" do
    @ds.select_hash(:a, :b).must_equal(1=>2, 5=>6)
    @ds.select_hash(:a__a___e, :b).must_equal(1=>2, 5=>6)
    @ds.select_hash(Sequel.expr(:a__a).as(:e), :b).must_equal(1=>2, 5=>6)
    @ds.select_hash(Sequel.qualify(:a, :a).as(:e), :b).must_equal(1=>2, 5=>6)
    @ds.select_hash(Sequel.identifier(:a).qualify(:a).as(:e), :b).must_equal(1=>2, 5=>6)
    @ds.select_hash([:a, :c], :b).must_equal([1, 3]=>2, [5, 7]=>6)
    @ds.select_hash(:a, [:b, :c]).must_equal(1=>[2, 3], 5=>[6, 7])
    @ds.select_hash([:a, :c], [:b, :d]).must_equal([1, 3]=>[2, 4], [5, 7]=>[6, 8])
  end

  it "should have working #select_hash_groups" do
    ds = @ds.order(*@ds.columns)
    ds.insert(1, 2, 3, 9)
    ds.select_hash_groups(:a, :d).must_equal(1=>[4, 9], 5=>[8])
    ds.select_hash_groups(:a__a___e, :d).must_equal(1=>[4, 9], 5=>[8])
    ds.select_hash_groups(Sequel.expr(:a__a).as(:e), :d).must_equal(1=>[4, 9], 5=>[8])
    ds.select_hash_groups(Sequel.qualify(:a, :a).as(:e), :d).must_equal(1=>[4, 9], 5=>[8])
    ds.select_hash_groups(Sequel.identifier(:a).qualify(:a).as(:e), :d).must_equal(1=>[4, 9], 5=>[8])
    ds.select_hash_groups([:a, :c], :d).must_equal([1, 3]=>[4, 9], [5, 7]=>[8])
    ds.select_hash_groups(:a, [:b, :d]).must_equal(1=>[[2, 4], [2, 9]], 5=>[[6, 8]])
    ds.select_hash_groups([:a, :c], [:b, :d]).must_equal([1, 3]=>[[2, 4], [2, 9]], [5, 7]=>[[6, 8]])
  end
end

describe "Sequel::Dataset DSL support" do
  before do
    @db = DB
    @db.create_table!(:a){Integer :a; Integer :b}
    @ds = @db[:a].order(:a)
  end
  after do
    @db.drop_table?(:a)
  end

  it "should work with standard mathematical operators" do
    @ds.insert(20, 10)
    @ds.get{a + b}.to_i.must_equal 30
    @ds.get{a - b}.to_i.must_equal 10
    @ds.get{a * b}.to_i.must_equal 200
    @ds.get{a / b}.to_i.must_equal 2
  end

  it "should work with bitwise AND and OR operators" do
    @ds.insert(3, 5)
    @ds.get{a.sql_number | b}.to_i.must_equal 7
    @ds.get{a.sql_number & b}.to_i.must_equal 1
    @ds.get{a.sql_number | b | 8}.to_i.must_equal 15
    @ds.get{a.sql_number & b & 8}.to_i.must_equal 0
  end

  it "should work with the bitwise compliment operator" do
    @ds.insert(-3, 3)
    @ds.get{~a.sql_number}.to_i.must_equal 2
    @ds.get{~b.sql_number}.to_i.must_equal(-4)
  end

  it "should work with the bitwise xor operator" do
    @ds.insert(3, 5)
    @ds.get{a.sql_number ^ b}.to_i.must_equal 6
    @ds.get{a.sql_number ^ b ^ 1}.to_i.must_equal 7
  end

  it "should work with the modulus operator" do
    @ds.insert(3, 5)
    @ds.get{a.sql_number % 4}.to_i.must_equal 3
    @ds.get{b.sql_number % 4}.to_i.must_equal 1
    @ds.get{a.sql_number % 4 % 2}.to_i.must_equal 1
  end

  it "should work with inequality operators" do
    @ds.insert(10, 11)
    @ds.insert(11, 11)
    @ds.insert(20, 19)
    @ds.insert(20, 20)
    @ds.filter{a > b}.select_order_map(:a).must_equal [20]
    @ds.filter{a >= b}.select_order_map(:a).must_equal [11, 20, 20]
    @ds.filter{a < b}.select_order_map(:a).must_equal [10]
    @ds.filter{a <= b}.select_order_map(:a).must_equal [10, 11, 20]
  end

  it "should work with casting and string concatentation" do
    @ds.insert(20, 20)
    @ds.get{Sequel.cast(a, String).sql_string + Sequel.cast(b, String)}.must_equal '2020'
  end

  it "should work with ordering" do
    @ds.insert(10, 20)
    @ds.insert(20, 10)
    @ds.order(:a, :b).all.must_equal [{:a=>10, :b=>20}, {:a=>20, :b=>10}]
    @ds.order(Sequel.asc(:a), Sequel.asc(:b)).all.must_equal [{:a=>10, :b=>20}, {:a=>20, :b=>10}]
    @ds.order(Sequel.desc(:a), Sequel.desc(:b)).all.must_equal [{:a=>20, :b=>10}, {:a=>10, :b=>20}]
  end

  it "should work with qualifying" do
    @ds.insert(10, 20)
    @ds.get(:a__b).must_equal 20
    @ds.get{a__b}.must_equal 20
    @ds.get(Sequel.qualify(:a, :b)).must_equal 20
  end

  it "should work with aliasing" do
    @ds.insert(10, 20)
    @ds.get(:a__b___c).must_equal 20
    @ds.get{a__b.as(c)}.must_equal 20
    @ds.get(Sequel.qualify(:a, :b).as(:c)).must_equal 20
    @ds.get(Sequel.as(:b, :c)).must_equal 20
  end

  it "should work with selecting all columns of a table" do
    @ds.insert(20, 10)
    @ds.select_all(:a).all.must_equal [{:a=>20, :b=>10}]
  end

  it "should work with ranges as hash values" do
    @ds.insert(20, 10)
    @ds.filter(:a=>(10..30)).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter(:a=>(25..30)).all.must_equal []
    @ds.filter(:a=>(10..15)).all.must_equal []
    @ds.exclude(:a=>(10..30)).all.must_equal []
    @ds.exclude(:a=>(25..30)).all.must_equal [{:a=>20, :b=>10}]
    @ds.exclude(:a=>(10..15)).all.must_equal [{:a=>20, :b=>10}]
  end

  it "should work with nil as hash value" do
    @ds.insert(20, nil)
    @ds.filter(:a=>nil).all.must_equal []
    @ds.filter(:b=>nil).all.must_equal [{:a=>20, :b=>nil}]
    @ds.exclude(:b=>nil).all.must_equal []
    @ds.exclude(:a=>nil).all.must_equal [{:a=>20, :b=>nil}]
  end

  it "should work with arrays as hash values" do
    @ds.insert(20, 10)
    @ds.filter(:a=>[10]).all.must_equal []
    @ds.filter(:a=>[20, 10]).all.must_equal [{:a=>20, :b=>10}]
    @ds.exclude(:a=>[10]).all.must_equal [{:a=>20, :b=>10}]
    @ds.exclude(:a=>[20, 10]).all.must_equal []
  end

  it "should work with ranges as hash values" do
    @ds.insert(20, 10)
    @ds.filter(:a=>(10..30)).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter(:a=>(25..30)).all.must_equal []
    @ds.filter(:a=>(10..15)).all.must_equal []
    @ds.exclude(:a=>(10..30)).all.must_equal []
    @ds.exclude(:a=>(25..30)).all.must_equal [{:a=>20, :b=>10}]
    @ds.exclude(:a=>(10..15)).all.must_equal [{:a=>20, :b=>10}]
  end

  it "should work with CASE statements" do
    @ds.insert(20, 10)
    @ds.filter(Sequel.case({{:a=>20}=>20}, 0) > 0).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter(Sequel.case({{:a=>15}=>20}, 0) > 0).all.must_equal []
    @ds.filter(Sequel.case({20=>20}, 0, :a) > 0).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter(Sequel.case({15=>20}, 0, :a) > 0).all.must_equal []
  end

  it "should work with multiple value arrays" do
    @ds.insert(20, 10)
    @ds.quote_identifiers = false
    @ds.filter([:a, :b]=>[[20, 10]]).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter([:a, :b]=>[[10, 20]]).all.must_equal []
    @ds.filter([:a, :b]=>[[20, 10], [1, 2]]).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter([:a, :b]=>[[10, 10], [20, 20]]).all.must_equal []

    @ds.exclude([:a, :b]=>[[20, 10]]).all.must_equal []
    @ds.exclude([:a, :b]=>[[10, 20]]).all.must_equal [{:a=>20, :b=>10}]
    @ds.exclude([:a, :b]=>[[20, 10], [1, 2]]).all.must_equal []
    @ds.exclude([:a, :b]=>[[10, 10], [20, 20]]).all.must_equal [{:a=>20, :b=>10}]
  end

  it "should work with IN/NOT in with datasets" do
    @ds.insert(20, 10)
    ds = @ds.unordered
    @ds.quote_identifiers = false

    @ds.filter(:a=>ds.select(:a)).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter(:a=>ds.select(:a).where(:a=>15)).all.must_equal []
    @ds.exclude(:a=>ds.select(:a)).all.must_equal []
    @ds.exclude(:a=>ds.select(:a).where(:a=>15)).all.must_equal [{:a=>20, :b=>10}]

    @ds.filter([:a, :b]=>ds.select(:a, :b)).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter([:a, :b]=>ds.select(:b, :a)).all.must_equal []
    @ds.exclude([:a, :b]=>ds.select(:a, :b)).all.must_equal []
    @ds.exclude([:a, :b]=>ds.select(:b, :a)).all.must_equal [{:a=>20, :b=>10}]

    @ds.filter([:a, :b]=>ds.select(:a, :b).where(:a=>15)).all.must_equal []
    @ds.exclude([:a, :b]=>ds.select(:a, :b).where(:a=>15)).all.must_equal [{:a=>20, :b=>10}]
  end

  it "should work empty arrays" do
    @ds.insert(20, 10)
    @ds.filter(:a=>[]).all.must_equal []
    @ds.exclude(:a=>[]).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter([:a, :b]=>[]).all.must_equal []
    @ds.exclude([:a, :b]=>[]).all.must_equal [{:a=>20, :b=>10}]
  end

  it "should work empty arrays with nulls" do
    @ds = @ds.extension(:empty_array_consider_nulls)
    @ds.insert(nil, nil)
    @ds.filter(:a=>[]).all.must_equal []
    @ds.exclude(:a=>[]).all.must_equal []
    @ds.filter([:a, :b]=>[]).all.must_equal []
    @ds.exclude([:a, :b]=>[]).all.must_equal []

    pr = proc{|r| r.is_a?(Integer) ? (r != 0) : r}
    pr[@ds.get(Sequel.expr(:a=>[]))].must_equal nil
    pr[@ds.get(~Sequel.expr(:a=>[]))].must_equal nil
    pr[@ds.get(Sequel.expr([:a, :b]=>[]))].must_equal nil
    pr[@ds.get(~Sequel.expr([:a, :b]=>[]))].must_equal nil
  end

  it "should work empty arrays with nulls and the empty_array_ignore_nulls extension" do
    ds = @ds
    ds.insert(nil, nil)
    ds.filter(:a=>[]).all.must_equal []
    ds.exclude(:a=>[]).all.must_equal [{:a=>nil, :b=>nil}]
    ds.filter([:a, :b]=>[]).all.must_equal []
    ds.exclude([:a, :b]=>[]).all.must_equal [{:a=>nil, :b=>nil}]

    pr = proc{|r| r.is_a?(Integer) ? (r != 0) : r}
    pr[ds.get(Sequel.expr(:a=>[]))].must_equal false
    pr[ds.get(~Sequel.expr(:a=>[]))].must_equal true
    pr[ds.get(Sequel.expr([:a, :b]=>[]))].must_equal false
    pr[ds.get(~Sequel.expr([:a, :b]=>[]))].must_equal true
  end

  it "should work multiple conditions" do
    @ds.insert(20, 10)
    @ds.filter(:a=>20, :b=>10).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter([[:a, 20], [:b, 10]]).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter({:a=>20}, {:b=>10}).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter(Sequel.|({:a=>20}, {:b=>5})).all.must_equal [{:a=>20, :b=>10}]
    @ds.filter(Sequel.~(:a=>10)).all.must_equal [{:a=>20, :b=>10}]
  end
end

describe "SQL Extract Function" do
  before do
    @db = DB
    @db.create_table!(:a){DateTime :a}
    @ds = @db[:a].order(:a)
  end
  after do
    @db.drop_table?(:a)
  end

  it "should return the part of the datetime asked for" do
    t = Time.now
    def @ds.supports_timestamp_timezones?() false end
    @ds.insert(t)
    @ds.get{a.extract(:year)}.must_equal t.year
    @ds.get{a.extract(:month)}.must_equal t.month
    @ds.get{a.extract(:day)}.must_equal t.day
    @ds.get{a.extract(:hour)}.must_equal t.hour
    @ds.get{a.extract(:minute)}.must_equal t.min
    @ds.get{a.extract(:second)}.to_i.must_equal t.sec
  end
end

describe "Dataset string methods" do
  before do
    @db = DB
    @db.create_table!(:a) do
      String :a
      String :b
    end
    @ds = @db[:a].order(:a)
  end
  after do
    @db.drop_table?(:a)
  end

  it "#grep should return matching rows" do
    @ds.insert('foo', 'bar')
    @ds.grep(:a, 'foo').all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.grep(:b, 'foo').all.must_equal []
    @ds.grep(:b, 'bar').all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.grep(:a, 'bar').all.must_equal []
    @ds.grep([:a, :b], %w'foo bar').all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.grep([:a, :b], %w'boo far').all.must_equal []
  end

  it "#grep should work with :all_patterns and :all_columns options" do
    @ds.insert('foo bar', ' ')
    @ds.insert('foo d', 'bar')
    @ds.insert('foo e', ' ')
    @ds.insert(' ', 'bar')
    @ds.insert('foo f', 'baz')
    @ds.insert('foo baz', 'bar baz')
    @ds.insert('foo boo', 'boo foo')

    @ds.grep([:a, :b], %w'%foo% %bar%', :all_patterns=>true).all.must_equal [{:a=>'foo bar', :b=>' '}, {:a=>'foo baz', :b=>'bar baz'}, {:a=>'foo d', :b=>'bar'}]
    @ds.grep([:a, :b], %w'%foo% %bar% %blob%', :all_patterns=>true).all.must_equal []

    @ds.grep([:a, :b], %w'%bar% %foo%', :all_columns=>true).all.must_equal [{:a=>"foo baz", :b=>"bar baz"}, {:a=>"foo boo", :b=>"boo foo"}, {:a=>"foo d", :b=>"bar"}]
    @ds.grep([:a, :b], %w'%baz%', :all_columns=>true).all.must_equal [{:a=>'foo baz', :b=>'bar baz'}]

    @ds.grep([:a, :b], %w'%baz% %foo%', :all_columns=>true, :all_patterns=>true).all.must_equal []
    @ds.grep([:a, :b], %w'%boo% %foo%', :all_columns=>true, :all_patterns=>true).all.must_equal [{:a=>'foo boo', :b=>'boo foo'}]
  end

  it "#like should return matching rows" do
    @ds.insert('foo', 'bar')
    @ds.filter(Sequel.expr(:a).like('foo')).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.filter(Sequel.expr(:a).like('bar')).all.must_equal []
    @ds.filter(Sequel.expr(:a).like('foo', 'bar')).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:a).like('foo')).all.must_equal []
    @ds.exclude(Sequel.expr(:a).like('bar')).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:a).like('foo', 'bar')).all.must_equal []
  end

  it "#like should be case sensitive" do
    @ds.insert('foo', 'bar')
    @ds.filter(Sequel.expr(:a).like('Foo')).all.must_equal []
    @ds.filter(Sequel.expr(:b).like('baR')).all.must_equal []
    @ds.filter(Sequel.expr(:a).like('FOO', 'BAR')).all.must_equal []
    @ds.exclude(Sequel.expr(:a).like('Foo')).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:b).like('baR')).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:a).like('FOO', 'BAR')).all.must_equal [{:a=>'foo', :b=>'bar'}]
  end

  it "#ilike should return matching rows, in a case insensitive manner" do
    @ds.insert('foo', 'bar')
    @ds.filter(Sequel.expr(:a).ilike('Foo')).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.filter(Sequel.expr(:a).ilike('baR')).all.must_equal []
    @ds.filter(Sequel.expr(:a).ilike('FOO', 'BAR')).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:a).ilike('Foo')).all.must_equal []
    @ds.exclude(Sequel.expr(:a).ilike('baR')).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:a).ilike('FOO', 'BAR')).all.must_equal []
  end

  it "#like with regexp return matching rows" do
    @ds.insert('foo', 'bar')
    @ds.filter(Sequel.expr(:a).like(/fo/)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.filter(Sequel.expr(:a).like(/fo$/)).all.must_equal []
    @ds.filter(Sequel.expr(:a).like(/fo/, /ar/)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:a).like(/fo/)).all.must_equal []
    @ds.exclude(Sequel.expr(:a).like(/fo$/)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:a).like(/fo/, /ar/)).all.must_equal []
  end

  it "#like with regexp should be case sensitive if regexp is case sensitive" do
    @ds.insert('foo', 'bar')
    @ds.filter(Sequel.expr(:a).like(/Fo/)).all.must_equal []
    @ds.filter(Sequel.expr(:b).like(/baR/)).all.must_equal []
    @ds.filter(Sequel.expr(:a).like(/FOO/, /BAR/)).all.must_equal []
    @ds.exclude(Sequel.expr(:a).like(/Fo/)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:b).like(/baR/)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:a).like(/FOO/, /BAR/)).all.must_equal [{:a=>'foo', :b=>'bar'}]

    @ds.filter(Sequel.expr(:a).like(/Fo/i)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.filter(Sequel.expr(:b).like(/baR/i)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.filter(Sequel.expr(:a).like(/FOO/i, /BAR/i)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:a).like(/Fo/i)).all.must_equal []
    @ds.exclude(Sequel.expr(:b).like(/baR/i)).all.must_equal []
    @ds.exclude(Sequel.expr(:a).like(/FOO/i, /BAR/i)).all.must_equal []
  end

  it "#ilike with regexp should return matching rows, in a case insensitive manner" do
    @ds.insert('foo', 'bar')
    @ds.filter(Sequel.expr(:a).ilike(/Fo/)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.filter(Sequel.expr(:b).ilike(/baR/)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.filter(Sequel.expr(:a).ilike(/FOO/, /BAR/)).all.must_equal [{:a=>'foo', :b=>'bar'}]
    @ds.exclude(Sequel.expr(:a).ilike(/Fo/)).all.must_equal []
    @ds.exclude(Sequel.expr(:b).ilike(/baR/)).all.must_equal []
    @ds.exclude(Sequel.expr(:a).ilike(/FOO/, /BAR/)).all.must_equal []
  end

  it "should work with strings created with Sequel.join" do
    @ds.insert('foo', 'bar')
    @ds.get(Sequel.join([:a, "bar"])).must_equal 'foobar'
    @ds.get(Sequel.join(["foo", :b], ' ')).must_equal 'foo bar'
  end
end

describe "Dataset identifier methods" do
  before(:all) do
    class ::String
      def uprev
        upcase.reverse
      end
    end
    @db = DB
    @db.create_table!(:a){Integer :ab}
    @db[:a].insert(1)
  end
  before do
    @ds = @db[:a].order(:ab)
  end
  after(:all) do
    @db.drop_table?(:a)
  end

  it "#identifier_output_method should change how identifiers are output" do
    @ds.identifier_output_method = :upcase
    @ds.first.must_equal(:AB=>1)
    @ds.identifier_output_method = :uprev
    @ds.first.must_equal(:BA=>1)
  end

  it "should work with a nil identifier_output_method" do
    @ds.identifier_output_method = nil
    [{:ab=>1}, {:AB=>1}].must_include(@ds.first)
  end

  it "should work when not quoting identifiers" do
    @ds.quote_identifiers = false
    @ds.first.must_equal(:ab=>1)
  end
end

describe "Dataset defaults and overrides" do
  before do
    @db = DB
    @db.create_table!(:a){Integer :a}
    @ds = @db[:a].order(:a).extension(:set_overrides)
  end
  after do
    @db.drop_table?(:a)
  end

  it "#set_defaults should set defaults that can be overridden" do
    @ds = @ds.set_defaults(:a=>10)
    @ds.insert
    @ds.insert(:a=>20)
    @ds.all.must_equal [{:a=>10}, {:a=>20}]
  end

  it "#set_overrides should set defaults that cannot be overridden" do
    @ds = @ds.set_overrides(:a=>10)
    @ds.insert
    @ds.insert(:a=>20)
    @ds.all.must_equal [{:a=>10}, {:a=>10}]
  end
end

describe "Emulated functions" do
  before do
    @db = DB
    @db.create_table!(:a){String :a}
    @ds = @db[:a]
  end
  after do
    @db.drop_table?(:a)
  end

  it "Sequel.char_length should return the length of characters in the string" do
    @ds.get(Sequel.char_length(:a)).must_equal nil
    @ds.insert(:a=>'foo')
    @ds.get(Sequel.char_length(:a)).must_equal 3
  end

  it "Sequel.char_length should return the length of characters in the string including trailing blanks" do
    @ds.insert(:a=>' foo22 ')
    @ds.get(Sequel.char_length(:a)).must_equal 7
  end

  it "Sequel.trim should return the string with spaces trimmed from both sides" do
    @ds.get(Sequel.trim(:a)).must_equal nil
    @ds.insert(:a=>'foo')
    @ds.get(Sequel.trim(:a)).must_equal 'foo'
  end

  it "Sequel.trim should return the string with spaces trimmed from both sides" do
    @ds.insert(:a=>' foo22 ')
    @ds.get(Sequel.trim(:a)).must_equal 'foo22'
  end
end

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Prepared Statements and Bound Arguments" do
  before do
    @db = DB
    @db.create_table!(:items) do
      primary_key :id
      integer :numb
    end
    @c = Class.new(Sequel::Model(:items))
    @ds = @db[:items]
    @ds.insert(:id=>1, :numb=>10)
    @pr = @ds.requires_placeholder_type_specifiers? ? proc{|i| :"#{i}__integer"} : proc{|i| i}
  end
  after do
    @db.drop_table?(:items)
  end

  it "should support bound variables when selecting" do
    @ds.filter(:numb=>:$n).call(:each, :n=>10){|h| h.must_equal(:id=>1, :numb=>10)}
    @ds.filter(:numb=>:$n).call(:select, :n=>10).must_equal [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).call(:all, :n=>10).must_equal [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).call(:first, :n=>10).must_equal(:id=>1, :numb=>10)
    @ds.filter(:numb=>:$n).call([:map, :numb], :n=>10).must_equal [10]
    @ds.filter(:numb=>:$n).call([:to_hash, :id, :numb], :n=>10).must_equal(1=>10)
    @ds.filter(:numb=>:$n).call([:to_hash_groups, :id, :numb], :n=>10).must_equal(1=>[10])
  end

  it "should support blocks for each, select, all, and map when using bound variables" do
    a = []
    @ds.filter(:numb=>:$n).call(:each, :n=>10){|r| r[:numb] *= 2; a << r}; a.must_equal [{:id=>1, :numb=>20}]
    @ds.filter(:numb=>:$n).call(:select, :n=>10){|r| r[:numb] *= 2}.must_equal [{:id=>1, :numb=>20}]
    @ds.filter(:numb=>:$n).call(:all, :n=>10){|r| r[:numb] *= 2}.must_equal [{:id=>1, :numb=>20}]
    @ds.filter(:numb=>:$n).call([:map], :n=>10){|r| r[:numb] * 2}.must_equal [20]
  end

  it "should support binding variables before the call with #bind" do
    @ds.filter(:numb=>:$n).bind(:n=>10).call(:select).must_equal [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>10).call(:all).must_equal [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>10).call(:first).must_equal(:id=>1, :numb=>10)

    @ds.bind(:n=>10).filter(:numb=>:$n).call(:select).must_equal [{:id=>1, :numb=>10}]
    @ds.bind(:n=>10).filter(:numb=>:$n).call(:all).must_equal [{:id=>1, :numb=>10}]
    @ds.bind(:n=>10).filter(:numb=>:$n).call(:first).must_equal(:id=>1, :numb=>10)
  end

  it "should allow overriding variables specified with #bind" do
    @ds.filter(:numb=>:$n).bind(:n=>1).call(:select, :n=>10).must_equal [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>1).call(:all, :n=>10).must_equal [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>1).call(:first, :n=>10).must_equal(:id=>1, :numb=>10)

    @ds.filter(:numb=>:$n).bind(:n=>1).bind(:n=>10).call(:select).must_equal [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>1).bind(:n=>10).call(:all).must_equal [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>1).bind(:n=>10).call(:first).must_equal(:id=>1, :numb=>10)
  end

  it "should support placeholder literal strings with call" do
    @ds.filter(Sequel.lit("numb = ?", :$n)).call(:select, :n=>10).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support named placeholder literal strings and handle multiple named placeholders correctly with call" do
    @ds.filter(Sequel.lit("numb = :n", :n=>:$n)).call(:select, :n=>10).must_equal [{:id=>1, :numb=>10}]
    @ds.insert(:id=>2, :numb=>20)
    @ds.insert(:id=>3, :numb=>30)
    @ds.filter(Sequel.lit("numb > :n1 AND numb < :n2 AND numb = :n3", :n3=>:$n3, :n2=>:$n2, :n1=>:$n1)).call(:select, :n3=>20, :n2=>30, :n1=>10).must_equal [{:id=>2, :numb=>20}]
  end

  it "should support datasets with static sql and placeholders with call" do
    @db["SELECT * FROM items WHERE numb = ?", :$n].call(:select, :n=>10).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support subselects with call" do
    @ds.filter(:id=>:$i).filter(:numb=>@ds.select(:numb).filter(:numb=>:$n)).filter(:id=>:$j).call(:select, :n=>10, :i=>1, :j=>1).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support subselects with exists with call" do
    @ds.filter(:id=>:$i).filter(@ds.select(:numb).filter(:numb=>:$n).exists).filter(:id=>:$j).call(:select, :n=>10, :i=>1, :j=>1).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support subselects with literal strings with call" do
    @ds.filter(:id=>:$i, :numb=>@ds.select(:numb).filter(Sequel.lit("numb = ?", :$n))).call(:select, :n=>10, :i=>1).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support subselects with static sql and placeholders with call" do
    @ds.filter(:id=>:$i, :numb=>@db["SELECT numb FROM items WHERE numb = ?", :$n]).call(:select, :n=>10, :i=>1).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support subselects of subselects with call" do
    @ds.filter(:id=>:$i).filter(:numb=>@ds.select(:numb).filter(:numb=>@ds.select(:numb).filter(:numb=>:$n))).filter(:id=>:$j).call(:select, :n=>10, :i=>1, :j=>1).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support using a bound variable for a limit and offset" do
    @ds.insert(:id=>2, :numb=>20)
    ds = @ds.limit(:$n, :$n2).order(:id)
    ds.call(:select, :n=>1, :n2=>0).must_equal [{:id=>1, :numb=>10}]
    ds.call(:select, :n=>1, :n2=>1).must_equal [{:id=>2, :numb=>20}]
    ds.call(:select, :n=>1, :n2=>2).must_equal []
    ds.call(:select, :n=>2, :n2=>0).must_equal [{:id=>1, :numb=>10}, {:id=>2, :numb=>20}]
    ds.call(:select, :n=>2, :n2=>1).must_equal [{:id=>2, :numb=>20}]
  end

  it "should support bound variables with insert" do
    @ds.call(:insert, {:n=>20}, :numb=>:$n)
    @ds.count.must_equal 2
    @ds.order(:id).map(:numb).must_equal [10, 20]
  end

  it "should have insert return primary key value when using bound arguments" do
    assert_nil(@ds.call(:insert, {:n=>20}, :numb=>:$n, :id=>2))
    @ds.filter(:id=>2).first[:numb].must_equal 20
  end

  it "should support bound variables with delete" do
    assert_nil(@ds.filter(:numb=>:$n).call(:delete, :n=>10))
    @ds.count.must_equal 0
  end

  it "should support prepared statements when selecting" do
    @ds.filter(:numb=>:$n).prepare(:each, :select_n)
    @db.call(:select_n, :n=>10){|h| h.must_equal(:id=>1, :numb=>10)}
    @ds.filter(:numb=>:$n).prepare(:select, :select_n)
    @db.call(:select_n, :n=>10).must_equal [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).prepare(:all, :select_n)
    @db.call(:select_n, :n=>10).must_equal [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).prepare(:first, :select_n)
    @db.call(:select_n, :n=>10).must_equal(:id=>1, :numb=>10)
    @ds.filter(:numb=>:$n).prepare([:map, :numb], :select_n)
    @db.call(:select_n, :n=>10).must_equal [10]
    @ds.filter(:numb=>:$n).prepare([:to_hash, :id, :numb], :select_n)
    @db.call(:select_n, :n=>10).must_equal(1=>10)
  end

  it "should support blocks for each, select, all, and map when using prepared statements" do
    a = []
    @ds.filter(:numb=>:$n).prepare(:each, :select_n).call(:n=>10){|r| r[:numb] *= 2; a << r}; a.must_equal [{:id=>1, :numb=>20}]
    a = []
    @db.call(:select_n, :n=>10){|r| r[:numb] *= 2; a << r}; a.must_equal [{:id=>1, :numb=>20}]
    @ds.filter(:numb=>:$n).prepare(:select, :select_n).call(:n=>10){|r| r[:numb] *= 2}.must_equal [{:id=>1, :numb=>20}]
    @db.call(:select_n, :n=>10){|r| r[:numb] *= 2}.must_equal [{:id=>1, :numb=>20}]
    @ds.filter(:numb=>:$n).prepare(:all, :select_n).call(:n=>10){|r| r[:numb] *= 2}.must_equal [{:id=>1, :numb=>20}]
    @db.call(:select_n, :n=>10){|r| r[:numb] *= 2}.must_equal [{:id=>1, :numb=>20}]
    @ds.filter(:numb=>:$n).prepare([:map], :select_n).call(:n=>10){|r| r[:numb] *= 2}.must_equal [20]
    @db.call(:select_n, :n=>10){|r| r[:numb] *= 2}.must_equal [20]
  end

  it "should support prepared statements being called multiple times with different arguments" do
    @ds.filter(:numb=>:$n).prepare(:select, :select_n)
    @db.call(:select_n, :n=>10).must_equal [{:id=>1, :numb=>10}]
    @db.call(:select_n, :n=>0).must_equal []
    @db.call(:select_n, :n=>10).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support placeholder literal strings with prepare" do
    @ds.filter(Sequel.lit("numb = ?", :$n)).prepare(:select, :seq_select).call(:n=>10).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support named placeholder literal strings and handle multiple named placeholders correctly with prepare" do
    @ds.filter(Sequel.lit("numb = :n", :n=>:$n)).prepare(:select, :seq_select).call(:n=>10).must_equal [{:id=>1, :numb=>10}]
    @ds.insert(:id=>2, :numb=>20)
    @ds.insert(:id=>3, :numb=>30)
    @ds.filter(Sequel.lit("numb > :n1 AND numb < :n2 AND numb = :n3", :n3=>:$n3, :n2=>:$n2, :n1=>:$n1)).call(:select, :n3=>20, :n2=>30, :n1=>10).must_equal [{:id=>2, :numb=>20}]
  end

  it "should support datasets with static sql and placeholders with prepare" do
    @db["SELECT * FROM items WHERE numb = ?", :$n].prepare(:select, :seq_select).call(:n=>10).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support subselects with prepare" do
    @ds.filter(:id=>:$i).filter(:numb=>@ds.select(:numb).filter(:numb=>:$n)).filter(:id=>:$j).prepare(:select, :seq_select).call(:n=>10, :i=>1, :j=>1).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support subselects with exists with prepare" do
    @ds.filter(:id=>:$i).filter(@ds.select(:numb).filter(:numb=>:$n).exists).filter(:id=>:$j).prepare(:select, :seq_select).call(:n=>10, :i=>1, :j=>1).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support subselects with literal strings with prepare" do
    @ds.filter(:id=>:$i, :numb=>@ds.select(:numb).filter(Sequel.lit("numb = ?", :$n))).prepare(:select, :seq_select).call(:n=>10, :i=>1).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support subselects with static sql and placeholders with prepare" do
    @ds.filter(:id=>:$i, :numb=>@db["SELECT numb FROM items WHERE numb = ?", :$n]).prepare(:select, :seq_select).call(:n=>10, :i=>1).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support subselects of subselects with prepare" do
    @ds.filter(:id=>:$i).filter(:numb=>@ds.select(:numb).filter(:numb=>@ds.select(:numb).filter(:numb=>:$n))).filter(:id=>:$j).prepare(:select, :seq_select).call(:n=>10, :i=>1, :j=>1).must_equal [{:id=>1, :numb=>10}]
  end

  it "should support using a prepared_statement for a limit and offset" do
    @ds.insert(:id=>2, :numb=>20)
    ps = @ds.limit(:$n, :$n2).order(:id).prepare(:select, :seq_select)
    ps.call(:n=>1, :n2=>0).must_equal [{:id=>1, :numb=>10}]
    ps.call(:n=>1, :n2=>1).must_equal [{:id=>2, :numb=>20}]
    ps.call(:n=>1, :n2=>2).must_equal []
    ps.call(:n=>2, :n2=>0).must_equal [{:id=>1, :numb=>10}, {:id=>2, :numb=>20}]
    ps.call(:n=>2, :n2=>1).must_equal [{:id=>2, :numb=>20}]
  end

  it "should support prepared statements with insert" do
    @ds.prepare(:insert, :insert_n, :numb=>:$n)
    @db.call(:insert_n, :n=>20)
    @ds.count.must_equal 2
    @ds.order(:id).map(:numb).must_equal [10, 20]
  end

  it "should have insert return primary key value when using prepared statements" do
    @ds.prepare(:insert, :insert_n, :numb=>:$n, :id=>2)
    assert_nil(@db.call(:insert_n, :id=>2, :n=>20))
    @ds.filter(:id=>2).first[:numb].must_equal 20
  end

  it "should support prepared statements with delete" do
    @ds.filter(:numb=>:$n).prepare(:delete, :delete_n)
    assert_nil(@db.call(:delete_n, :n=>10))
    @ds.count.must_equal 0
  end

  it "model datasets should return model instances when using select, all, and first with bound variables" do
    @c.filter(:numb=>:$n).call(:select, :n=>10).must_equal [@c.load(:id=>1, :numb=>10)]
    @c.filter(:numb=>:$n).call(:all, :n=>10).must_equal [@c.load(:id=>1, :numb=>10)]
    @c.filter(:numb=>:$n).call(:first, :n=>10).must_equal @c.load(:id=>1, :numb=>10)
  end

  it "model datasets should return model instances when using select, all, and first with prepared statements" do
    @c.filter(:numb=>:$n).prepare(:select, :select_n1)
    @db.call(:select_n1, :n=>10).must_equal [@c.load(:id=>1, :numb=>10)]
    @c.filter(:numb=>:$n).prepare(:all, :select_n1)
    @db.call(:select_n1, :n=>10).must_equal [@c.load(:id=>1, :numb=>10)]
    @c.filter(:numb=>:$n).prepare(:first, :select_n1)
    @db.call(:select_n1, :n=>10).must_equal @c.load(:id=>1, :numb=>10)
  end
end

describe "Bound Argument Types" do
  before do
    @db = DB
    @db.create_table!(:items) do
      primary_key :id
      DateTime :dt
      String :s
      Time :t
      Float :f
      TrueClass :b
    end
    @ds = @db[:items]
    @vs = {:id=>1, :dt=>DateTime.civil(2010, 10, 12, 13, 14, 15), :f=>1.0, :s=>'str', :t=>Time.at(20101010), :b=>true}
    @ds.insert(@vs)
  end
  after do
    @db.drop_table?(:items)
  end

  it "should handle float type" do
    @ds.filter(:f=>:$x).prepare(:first, :ps_float).call(:x=>@vs[:f])[:f].must_equal @vs[:f]
  end

  it "should handle string type" do
    @ds.filter(:s=>:$x).prepare(:first, :ps_string).call(:x=>@vs[:s])[:s].must_equal @vs[:s]
  end

  it "should handle boolean type" do
    @ds.filter(:b=>:$x).prepare(:first, :ps_string).call(:x=>@vs[:b])[:b].must_equal @vs[:b]
  end
end

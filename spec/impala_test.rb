require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

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
    @ds.get{date_add(t, 0)}.to_date.must_equal (Date.today)
    @ds.get{date_add(t, Sequel.lit('interval 0 days'))}.to_date.must_equal (Date.today)
    @ds.get{date_add(t, 1)}.to_date.must_equal (Date.today+1)
    @ds.get{date_add(t, Sequel.lit('interval 1 day'))}.to_date.must_equal (Date.today+1)
    @ds.get{date_add(t, -1)}.to_date.must_equal (Date.today-1)
    @ds.get{date_add(t, Sequel.lit('interval -1 day'))}.to_date.must_equal (Date.today-1)
  end
end

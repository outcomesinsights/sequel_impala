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

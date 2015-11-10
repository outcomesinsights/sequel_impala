require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "date_arithmetic extension" do
  asd = begin
    require 'active_support/duration'
    require 'active_support/inflector'
    require 'active_support/core_ext/string/inflections'
    true
  rescue LoadError
    false
  end

  before(:all) do
    @db = DB
    @db.extension(:date_arithmetic)
    if @db.database_type == :sqlite
      @db.use_timestamp_timezones = false
    end
    @date = Date.civil(2010, 7, 12)
    @dt = Time.local(2010, 7, 12)
    if asd
      @d0 = ActiveSupport::Duration.new(0, [[:days, 0]])
      @d1 = ActiveSupport::Duration.new(1, [[:days, 1]])
      @d2 = ActiveSupport::Duration.new(1, [[:years, 1], [:months, 1], [:days, 1], [:minutes, 61], [:seconds, 1]])
    end
    @h0 = {:days=>0}
    @h1 = {:days=>1, :years=>nil, :hours=>0}
    @h2 = {:years=>1, :months=>1, :days=>1, :hours=>1, :minutes=>1, :seconds=>1}
    @a1 = Time.local(2010, 7, 13)
    @a2 = Time.local(2011, 8, 13, 1, 1, 1)
    @s1 = Time.local(2010, 7, 11)
    @s2 = Time.local(2009, 6, 10, 22, 58, 59)
    @check = lambda do |meth, in_date, in_interval, should|
      output = @db.get(Sequel.send(meth, in_date, in_interval))
      output = Time.parse(output.to_s) unless output.is_a?(Time) || output.is_a?(DateTime)
      output.year.must_equal should.year
      output.month.must_equal should.month
      output.day.must_equal should.day
      output.hour.must_equal should.hour
      output.min.must_equal should.min
      output.sec.must_equal should.sec
    end
  end
  after(:all) do
    if @db.database_type == :sqlite
      @db.use_timestamp_timezones = true
    end
  end

  if asd
    it "be able to use Sequel.date_add to add ActiveSupport::Duration objects to dates and datetimes" do
      @check.call(:date_add, @date, @d0, @dt)
      @check.call(:date_add, @date, @d1, @a1)
      @check.call(:date_add, @date, @d2, @a2)

      @check.call(:date_add, @dt, @d0, @dt)
      @check.call(:date_add, @dt, @d1, @a1)
      @check.call(:date_add, @dt, @d2, @a2)
    end

    it "be able to use Sequel.date_sub to subtract ActiveSupport::Duration objects from dates and datetimes" do
      @check.call(:date_sub, @date, @d0, @dt)
      @check.call(:date_sub, @date, @d1, @s1)
      @check.call(:date_sub, @date, @d2, @s2)

      @check.call(:date_sub, @dt, @d0, @dt)
      @check.call(:date_sub, @dt, @d1, @s1)
      @check.call(:date_sub, @dt, @d2, @s2)
    end
  end

  it "be able to use Sequel.date_add to add interval hashes to dates and datetimes" do
    @check.call(:date_add, @date, @h0, @dt)
    @check.call(:date_add, @date, @h1, @a1)
    @check.call(:date_add, @date, @h2, @a2)

    @check.call(:date_add, @dt, @h0, @dt)
    @check.call(:date_add, @dt, @h1, @a1)
    @check.call(:date_add, @dt, @h2, @a2)
  end

  it "be able to use Sequel.date_sub to subtract interval hashes from dates and datetimes" do
    @check.call(:date_sub, @date, @h0, @dt)
    @check.call(:date_sub, @date, @h1, @s1)
    @check.call(:date_sub, @date, @h2, @s2)

    @check.call(:date_sub, @dt, @h0, @dt)
    @check.call(:date_sub, @dt, @h1, @s1)
    @check.call(:date_sub, @dt, @h2, @s2)
  end
end

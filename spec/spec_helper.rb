$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))
require 'logger'
require 'sequel'

gem 'minitest'
require 'minitest/autorun'
require 'minitest/hooks/default'
require 'minitest/shared_description'

Sequel::Model.cache_associations = false if ENV['SEQUEL_NO_CACHE_ASSOCIATIONS']
Sequel::Model.cache_anonymous_models = false

unless defined?(DB)
  DB = Sequel.connect(ENV['IMPALA_URL'] || 'jdbc:hive2://localhost:21050/;auth=noSasl')
end

IDENTIFIER_MANGLING = !!ENV['IDENTIFIER_MANGLING'] unless defined?(IDENTIFIER_MANGLING)
DB.extension(:identifier_mangling) if IDENTIFIER_MANGLING

class Minitest::HooksSpec
  if ENV['TESTS_DIV_MOD']
    div, mod = ENV['TESTS_DIV_MOD'].split(' ', 2).map(&:to_i)
    raise "invalid TESTS_DIV_MOD div: #{div}" unless div >= 2
    raise "invalid TESTS_DIV_MOD mod: #{mod} (div: #{div})" unless mod >= 0 && mod < div
    test_number = -1
    inc = proc{test_number+=1}
    
    define_singleton_method(:it) do |*a, &block|
      if inc.call % div == mod
        super(*a, &block)
      end
    end
  end

  def log
    begin
      DB.loggers << Logger.new(STDOUT)
      yield
    ensure
     DB.loggers.pop
    end
  end
end


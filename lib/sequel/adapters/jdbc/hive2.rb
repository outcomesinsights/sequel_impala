require 'sequel/adapters/shared/impala'

Sequel::JDBC.load_driver('org.apache.hive.jdbc.HiveDriver', :Hive2)

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:hive2] = proc do |db|
        db.extend(Sequel::JDBC::Hive2::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::Hive2::Dataset
        org.apache.hive.jdbc.HiveDriver
      end
    end

    module Hive2
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::Impala::DatabaseMethods
      end

      class Dataset < JDBC::Dataset
        include Sequel::Impala::DatasetMethods
      end
    end
  end
end

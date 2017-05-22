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
        include Sequel::Impala::DatabaseMethods

        # Recognize wrapped java.net.SocketExceptions as disconnect errors
        def disconnect_error?(exception, opts)
          super || exception.message =~ /\AJava::JavaSql::SQLException: org\.apache\.thrift\.transport\.TTransportException: java\.net\.SocketException/
        end
      end

      class Dataset < JDBC::Dataset
        include Sequel::Impala::DatasetMethods
      end
    end
  end
end

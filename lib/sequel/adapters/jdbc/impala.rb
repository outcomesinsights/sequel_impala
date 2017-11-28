require 'sequel/adapters/shared/impala'

Sequel::JDBC.load_driver('com.cloudera.impala.jdbc41.Driver', :Impala)

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:impala] = proc do |db|
        db.extend(Sequel::JDBC::Impala::DatabaseMethods)
        db.extend_datasets(Sequel::Impala::DatasetMethods)
        com.cloudera.impala.jdbc41.Driver
      end
    end

    module Impala
      module DatabaseMethods
        include Sequel::Impala::DatabaseMethods

        # Recognize wrapped and unwrapped java.net.SocketExceptions as disconnect errors
        def disconnect_error?(exception, opts)
          super || exception.message =~ /\A(Java::JavaSql::SQLException: )?org\.apache\.thrift\.transport\.TTransportException: java\.net\.SocketException/
        end

        def disconnect_connection(c)
          super
        rescue java.sql.SQLException
          nil
        end
      end
    end
  end
end


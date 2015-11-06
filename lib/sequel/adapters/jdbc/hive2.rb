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
      module Invalid
        def invalid(meth, msg)
          define_method(meth) do |*|
            raise InvalidOperation, msg
          end
        end
      end

      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        extend Invalid

        invalid :transaction, "Impala does not support transactions"
        invalid :serial_primary_key_options, "Impala does not support auto incrementing primary keys"

        def database_type
          :impala
        end

        def supports_create_table_if_not_exists?
          true
        end

        private

        def quote_identifiers_default
          false
        end
      
        def identifier_input_method_default
          nil
        end
      
        def identifier_output_method_default
          nil
        end
      end
      
      class Dataset < JDBC::Dataset
        extend Invalid

        invalid :update, "Impala does not support UPDATE"
        invalid :delete, "Impala does not support DELETE"
        invalid :truncate, "Impala does not support TRUNCATE or DELETE"
      end
    end
  end
end

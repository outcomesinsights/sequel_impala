module Sequel
  module Impala
    module Invalid
      def invalid(meth, msg)
        define_method(meth) do |*|
          raise InvalidOperation, msg
        end
      end
    end

    module DatabaseMethods
      extend Invalid

      def database_type
        :impala
      end

      def supports_create_table_if_not_exists?
        true
      end

      def transaction(opts=OPTS)
        synchronize(opts[:server]) do |c|
          yield c
        end
      end

      def serial_primary_key_options
        {:type=>Integer}
      end
      
      def identifier_input_method_default
        nil
      end
     
      def identifier_output_method_default
        nil
      end
    end

    module DatasetMethods
      extend Invalid

      BACKTICK = '`'.freeze
      DOUBLE_BACKTICK = '``'.freeze

      invalid :update, "Impala does not support UPDATE"
      invalid :delete, "Impala does not support DELETE"
      invalid :truncate, "Impala does not support TRUNCATE or DELETE"

      private

      def quoted_identifier_append(sql, name)
        sql << BACKTICK<< name.to_s.gsub(BACKTICK, DOUBLE_BACKTICK) << BACKTICK
      end
    end
  end
end

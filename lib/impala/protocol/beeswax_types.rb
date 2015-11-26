#
# Autogenerated by Thrift Compiler (0.9.1)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

require 'thrift'
require 'hive_metastore_types'


module Impala
  module Protocol
    module Beeswax
      module QueryState
        CREATED = 0
        INITIALIZED = 1
        COMPILED = 2
        RUNNING = 3
        FINISHED = 4
        EXCEPTION = 5
        VALUE_MAP = {0 => "CREATED", 1 => "INITIALIZED", 2 => "COMPILED", 3 => "RUNNING", 4 => "FINISHED", 5 => "EXCEPTION"}
        VALID_VALUES = Set.new([CREATED, INITIALIZED, COMPILED, RUNNING, FINISHED, EXCEPTION]).freeze
      end

      class Query
        include ::Thrift::Struct, ::Thrift::Struct_Union
        QUERY = 1
        CONFIGURATION = 3
        HADOOP_USER = 4

        FIELDS = {
          QUERY => {:type => ::Thrift::Types::STRING, :name => 'query'},
          CONFIGURATION => {:type => ::Thrift::Types::LIST, :name => 'configuration', :element => {:type => ::Thrift::Types::STRING}},
          HADOOP_USER => {:type => ::Thrift::Types::STRING, :name => 'hadoop_user'}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class QueryHandle
        include ::Thrift::Struct, ::Thrift::Struct_Union
        ID = 1
        LOG_CONTEXT = 2

        FIELDS = {
          ID => {:type => ::Thrift::Types::STRING, :name => 'id'},
          LOG_CONTEXT => {:type => ::Thrift::Types::STRING, :name => 'log_context'}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class QueryExplanation
        include ::Thrift::Struct, ::Thrift::Struct_Union
        TEXTUAL = 1

        FIELDS = {
          TEXTUAL => {:type => ::Thrift::Types::STRING, :name => 'textual'}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class Results
        include ::Thrift::Struct, ::Thrift::Struct_Union
        READY = 1
        COLUMNS = 2
        DATA = 3
        START_ROW = 4
        HAS_MORE = 5

        FIELDS = {
          READY => {:type => ::Thrift::Types::BOOL, :name => 'ready'},
          COLUMNS => {:type => ::Thrift::Types::LIST, :name => 'columns', :element => {:type => ::Thrift::Types::STRING}},
          DATA => {:type => ::Thrift::Types::LIST, :name => 'data', :element => {:type => ::Thrift::Types::STRING}},
          START_ROW => {:type => ::Thrift::Types::I64, :name => 'start_row'},
          HAS_MORE => {:type => ::Thrift::Types::BOOL, :name => 'has_more'}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      # Metadata information about the results.
# Applicable only for SELECT.
      class ResultsMetadata
        include ::Thrift::Struct, ::Thrift::Struct_Union
        SCHEMA = 1
        TABLE_DIR = 2
        IN_TABLENAME = 3
        DELIM = 4

        FIELDS = {
          # The schema of the results
          SCHEMA => {:type => ::Thrift::Types::STRUCT, :name => 'schema', :class => ::Impala::Protocol::HiveMetastore::Schema},
          # The directory containing the results. Not applicable for partition table.
          TABLE_DIR => {:type => ::Thrift::Types::STRING, :name => 'table_dir'},
          # If the results are straight from an existing table, the table name.
          IN_TABLENAME => {:type => ::Thrift::Types::STRING, :name => 'in_tablename'},
          # Field delimiter
          DELIM => {:type => ::Thrift::Types::STRING, :name => 'delim'}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class BeeswaxException < ::Thrift::Exception
        include ::Thrift::Struct, ::Thrift::Struct_Union
        MESSAGE = 1
        LOG_CONTEXT = 2
        HANDLE = 3
        ERRORCODE = 4
        SQLSTATE = 5

        FIELDS = {
          MESSAGE => {:type => ::Thrift::Types::STRING, :name => 'message'},
          LOG_CONTEXT => {:type => ::Thrift::Types::STRING, :name => 'log_context'},
          HANDLE => {:type => ::Thrift::Types::STRUCT, :name => 'handle', :class => ::Impala::Protocol::Beeswax::QueryHandle},
          ERRORCODE => {:type => ::Thrift::Types::I32, :name => 'errorCode', :default => 0, :optional => true},
          SQLSTATE => {:type => ::Thrift::Types::STRING, :name => 'SQLState', :default => %q"     ", :optional => true}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class QueryNotFoundException < ::Thrift::Exception
        include ::Thrift::Struct, ::Thrift::Struct_Union

        FIELDS = {

        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      # Represents a Hadoop-style configuration variable.
      class ConfigVariable
        include ::Thrift::Struct, ::Thrift::Struct_Union
        KEY = 1
        VALUE = 2
        DESCRIPTION = 3

        FIELDS = {
          KEY => {:type => ::Thrift::Types::STRING, :name => 'key'},
          VALUE => {:type => ::Thrift::Types::STRING, :name => 'value'},
          DESCRIPTION => {:type => ::Thrift::Types::STRING, :name => 'description'}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

    end
  end
end

#
# Autogenerated by Thrift Compiler (0.9.3)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

require 'thrift'

module Impala
  module Protocol
    module TPrimitiveType
      INVALID_TYPE = 0
      NULL_TYPE = 1
      BOOLEAN = 2
      TINYINT = 3
      SMALLINT = 4
      INT = 5
      BIGINT = 6
      FLOAT = 7
      DOUBLE = 8
      DATE = 9
      DATETIME = 10
      TIMESTAMP = 11
      STRING = 12
      BINARY = 13
      DECIMAL = 14
      CHAR = 15
      VARCHAR = 16
      VALUE_MAP = {0 => "INVALID_TYPE", 1 => "NULL_TYPE", 2 => "BOOLEAN", 3 => "TINYINT", 4 => "SMALLINT", 5 => "INT", 6 => "BIGINT", 7 => "FLOAT", 8 => "DOUBLE", 9 => "DATE", 10 => "DATETIME", 11 => "TIMESTAMP", 12 => "STRING", 13 => "BINARY", 14 => "DECIMAL", 15 => "CHAR", 16 => "VARCHAR"}
      VALID_VALUES = Set.new([INVALID_TYPE, NULL_TYPE, BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DATE, DATETIME, TIMESTAMP, STRING, BINARY, DECIMAL, CHAR, VARCHAR]).freeze
    end

    module TTypeNodeType
      SCALAR = 0
      ARRAY = 1
      MAP = 2
      STRUCT = 3
      VALUE_MAP = {0 => "SCALAR", 1 => "ARRAY", 2 => "MAP", 3 => "STRUCT"}
      VALID_VALUES = Set.new([SCALAR, ARRAY, MAP, STRUCT]).freeze
    end

    module TStmtType
      QUERY = 0
      DDL = 1
      DML = 2
      EXPLAIN = 3
      LOAD = 4
      SET = 5
      VALUE_MAP = {0 => "QUERY", 1 => "DDL", 2 => "DML", 3 => "EXPLAIN", 4 => "LOAD", 5 => "SET"}
      VALID_VALUES = Set.new([QUERY, DDL, DML, EXPLAIN, LOAD, SET]).freeze
    end

    module TExplainLevel
      MINIMAL = 0
      STANDARD = 1
      EXTENDED = 2
      VERBOSE = 3
      VALUE_MAP = {0 => "MINIMAL", 1 => "STANDARD", 2 => "EXTENDED", 3 => "VERBOSE"}
      VALID_VALUES = Set.new([MINIMAL, STANDARD, EXTENDED, VERBOSE]).freeze
    end

    module TRuntimeFilterMode
      OFF = 0
      LOCAL = 1
      GLOBAL = 2
      VALUE_MAP = {0 => "OFF", 1 => "LOCAL", 2 => "GLOBAL"}
      VALID_VALUES = Set.new([OFF, LOCAL, GLOBAL]).freeze
    end

    module TFunctionCategory
      SCALAR = 0
      AGGREGATE = 1
      ANALYTIC = 2
      VALUE_MAP = {0 => "SCALAR", 1 => "AGGREGATE", 2 => "ANALYTIC"}
      VALID_VALUES = Set.new([SCALAR, AGGREGATE, ANALYTIC]).freeze
    end

    module TFunctionBinaryType
      BUILTIN = 0
      JAVA = 1
      NATIVE = 2
      IR = 3
      VALUE_MAP = {0 => "BUILTIN", 1 => "JAVA", 2 => "NATIVE", 3 => "IR"}
      VALID_VALUES = Set.new([BUILTIN, JAVA, NATIVE, IR]).freeze
    end

    class TScalarType
      include ::Thrift::Struct, ::Thrift::Struct_Union
      TYPE = 1
      LEN = 2
      PRECISION = 3
      SCALE = 4

      FIELDS = {
        TYPE => {:type => ::Thrift::Types::I32, :name => 'type', :enum_class => ::Impala::Protocol::TPrimitiveType},
        LEN => {:type => ::Thrift::Types::I32, :name => 'len', :optional => true},
        PRECISION => {:type => ::Thrift::Types::I32, :name => 'precision', :optional => true},
        SCALE => {:type => ::Thrift::Types::I32, :name => 'scale', :optional => true}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field type is unset!') unless @type
        unless @type.nil? || ::Impala::Protocol::TPrimitiveType::VALID_VALUES.include?(@type)
          raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Invalid value of field type!')
        end
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TStructField
      include ::Thrift::Struct, ::Thrift::Struct_Union
      NAME = 1
      COMMENT = 2

      FIELDS = {
        NAME => {:type => ::Thrift::Types::STRING, :name => 'name'},
        COMMENT => {:type => ::Thrift::Types::STRING, :name => 'comment', :optional => true}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field name is unset!') unless @name
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TTypeNode
      include ::Thrift::Struct, ::Thrift::Struct_Union
      TYPE = 1
      SCALAR_TYPE = 2
      STRUCT_FIELDS = 3

      FIELDS = {
        TYPE => {:type => ::Thrift::Types::I32, :name => 'type', :enum_class => ::Impala::Protocol::TTypeNodeType},
        SCALAR_TYPE => {:type => ::Thrift::Types::STRUCT, :name => 'scalar_type', :class => ::Impala::Protocol::TScalarType, :optional => true},
        STRUCT_FIELDS => {:type => ::Thrift::Types::LIST, :name => 'struct_fields', :element => {:type => ::Thrift::Types::STRUCT, :class => ::Impala::Protocol::TStructField}, :optional => true}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field type is unset!') unless @type
        unless @type.nil? || ::Impala::Protocol::TTypeNodeType::VALID_VALUES.include?(@type)
          raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Invalid value of field type!')
        end
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TColumnType
      include ::Thrift::Struct, ::Thrift::Struct_Union
      TYPES = 1

      FIELDS = {
        TYPES => {:type => ::Thrift::Types::LIST, :name => 'types', :element => {:type => ::Thrift::Types::STRUCT, :class => ::Impala::Protocol::TTypeNode}}
      }

      def struct_fields; FIELDS; end

      def validate
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TNetworkAddress
      include ::Thrift::Struct, ::Thrift::Struct_Union
      HOSTNAME = 1
      PORT = 2

      FIELDS = {
        HOSTNAME => {:type => ::Thrift::Types::STRING, :name => 'hostname'},
        PORT => {:type => ::Thrift::Types::I32, :name => 'port'}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field hostname is unset!') unless @hostname
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field port is unset!') unless @port
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TUniqueId
      include ::Thrift::Struct, ::Thrift::Struct_Union
      HI = 1
      LO = 2

      FIELDS = {
        HI => {:type => ::Thrift::Types::I64, :name => 'hi'},
        LO => {:type => ::Thrift::Types::I64, :name => 'lo'}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field hi is unset!') unless @hi
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field lo is unset!') unless @lo
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TFunctionName
      include ::Thrift::Struct, ::Thrift::Struct_Union
      DB_NAME = 1
      FUNCTION_NAME = 2

      FIELDS = {
        DB_NAME => {:type => ::Thrift::Types::STRING, :name => 'db_name', :optional => true},
        FUNCTION_NAME => {:type => ::Thrift::Types::STRING, :name => 'function_name'}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field function_name is unset!') unless @function_name
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TScalarFunction
      include ::Thrift::Struct, ::Thrift::Struct_Union
      SYMBOL = 1
      PREPARE_FN_SYMBOL = 2
      CLOSE_FN_SYMBOL = 3

      FIELDS = {
        SYMBOL => {:type => ::Thrift::Types::STRING, :name => 'symbol'},
        PREPARE_FN_SYMBOL => {:type => ::Thrift::Types::STRING, :name => 'prepare_fn_symbol', :optional => true},
        CLOSE_FN_SYMBOL => {:type => ::Thrift::Types::STRING, :name => 'close_fn_symbol', :optional => true}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field symbol is unset!') unless @symbol
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TAggregateFunction
      include ::Thrift::Struct, ::Thrift::Struct_Union
      INTERMEDIATE_TYPE = 1
      UPDATE_FN_SYMBOL = 2
      INIT_FN_SYMBOL = 3
      SERIALIZE_FN_SYMBOL = 4
      MERGE_FN_SYMBOL = 5
      FINALIZE_FN_SYMBOL = 6
      GET_VALUE_FN_SYMBOL = 8
      REMOVE_FN_SYMBOL = 9
      IGNORES_DISTINCT = 7

      FIELDS = {
        INTERMEDIATE_TYPE => {:type => ::Thrift::Types::STRUCT, :name => 'intermediate_type', :class => ::Impala::Protocol::TColumnType},
        UPDATE_FN_SYMBOL => {:type => ::Thrift::Types::STRING, :name => 'update_fn_symbol'},
        INIT_FN_SYMBOL => {:type => ::Thrift::Types::STRING, :name => 'init_fn_symbol'},
        SERIALIZE_FN_SYMBOL => {:type => ::Thrift::Types::STRING, :name => 'serialize_fn_symbol', :optional => true},
        MERGE_FN_SYMBOL => {:type => ::Thrift::Types::STRING, :name => 'merge_fn_symbol', :optional => true},
        FINALIZE_FN_SYMBOL => {:type => ::Thrift::Types::STRING, :name => 'finalize_fn_symbol', :optional => true},
        GET_VALUE_FN_SYMBOL => {:type => ::Thrift::Types::STRING, :name => 'get_value_fn_symbol', :optional => true},
        REMOVE_FN_SYMBOL => {:type => ::Thrift::Types::STRING, :name => 'remove_fn_symbol', :optional => true},
        IGNORES_DISTINCT => {:type => ::Thrift::Types::BOOL, :name => 'ignores_distinct', :optional => true}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field intermediate_type is unset!') unless @intermediate_type
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field update_fn_symbol is unset!') unless @update_fn_symbol
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field init_fn_symbol is unset!') unless @init_fn_symbol
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TFunction
      include ::Thrift::Struct, ::Thrift::Struct_Union
      NAME = 1
      BINARY_TYPE = 2
      ARG_TYPES = 3
      RET_TYPE = 4
      HAS_VAR_ARGS = 5
      COMMENT = 6
      SIGNATURE = 7
      HDFS_LOCATION = 8
      SCALAR_FN = 9
      AGGREGATE_FN = 10
      IS_PERSISTENT = 11

      FIELDS = {
        NAME => {:type => ::Thrift::Types::STRUCT, :name => 'name', :class => ::Impala::Protocol::TFunctionName},
        BINARY_TYPE => {:type => ::Thrift::Types::I32, :name => 'binary_type', :enum_class => ::Impala::Protocol::TFunctionBinaryType},
        ARG_TYPES => {:type => ::Thrift::Types::LIST, :name => 'arg_types', :element => {:type => ::Thrift::Types::STRUCT, :class => ::Impala::Protocol::TColumnType}},
        RET_TYPE => {:type => ::Thrift::Types::STRUCT, :name => 'ret_type', :class => ::Impala::Protocol::TColumnType},
        HAS_VAR_ARGS => {:type => ::Thrift::Types::BOOL, :name => 'has_var_args'},
        COMMENT => {:type => ::Thrift::Types::STRING, :name => 'comment', :optional => true},
        SIGNATURE => {:type => ::Thrift::Types::STRING, :name => 'signature', :optional => true},
        HDFS_LOCATION => {:type => ::Thrift::Types::STRING, :name => 'hdfs_location', :optional => true},
        SCALAR_FN => {:type => ::Thrift::Types::STRUCT, :name => 'scalar_fn', :class => ::Impala::Protocol::TScalarFunction, :optional => true},
        AGGREGATE_FN => {:type => ::Thrift::Types::STRUCT, :name => 'aggregate_fn', :class => ::Impala::Protocol::TAggregateFunction, :optional => true},
        IS_PERSISTENT => {:type => ::Thrift::Types::BOOL, :name => 'is_persistent', :optional => true}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field name is unset!') unless @name
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field binary_type is unset!') unless @binary_type
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field arg_types is unset!') unless @arg_types
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field ret_type is unset!') unless @ret_type
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field has_var_args is unset!') if @has_var_args.nil?
        unless @binary_type.nil? || ::Impala::Protocol::TFunctionBinaryType::VALID_VALUES.include?(@binary_type)
          raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Invalid value of field binary_type!')
        end
      end

      ::Thrift::Struct.generate_accessors self
    end

  end
end

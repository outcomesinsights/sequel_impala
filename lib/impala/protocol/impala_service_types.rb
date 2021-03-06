#
# Autogenerated by Thrift Compiler (0.9.3)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

require 'thrift'
require 'exec_stats_types'
require 'status_types'
require 'types_types'
require 'beeswax_types'
require 't_c_l_i_service_types'


module Impala
  module Protocol
    module TImpalaQueryOptions
      ABORT_ON_ERROR = 0
      MAX_ERRORS = 1
      DISABLE_CODEGEN = 2
      BATCH_SIZE = 3
      MEM_LIMIT = 4
      NUM_NODES = 5
      MAX_SCAN_RANGE_LENGTH = 6
      MAX_IO_BUFFERS = 7
      NUM_SCANNER_THREADS = 8
      ALLOW_UNSUPPORTED_FORMATS = 9
      DEFAULT_ORDER_BY_LIMIT = 10
      DEBUG_ACTION = 11
      ABORT_ON_DEFAULT_LIMIT_EXCEEDED = 12
      COMPRESSION_CODEC = 13
      SEQ_COMPRESSION_MODE = 14
      HBASE_CACHING = 15
      HBASE_CACHE_BLOCKS = 16
      PARQUET_FILE_SIZE = 17
      EXPLAIN_LEVEL = 18
      SYNC_DDL = 19
      REQUEST_POOL = 20
      V_CPU_CORES = 21
      RESERVATION_REQUEST_TIMEOUT = 22
      DISABLE_CACHED_READS = 23
      DISABLE_OUTERMOST_TOPN = 24
      RM_INITIAL_MEM = 25
      QUERY_TIMEOUT_S = 26
      MAX_BLOCK_MGR_MEMORY = 27
      APPX_COUNT_DISTINCT = 28
      DISABLE_UNSAFE_SPILLS = 29
      EXEC_SINGLE_NODE_ROWS_THRESHOLD = 30
      OPTIMIZE_PARTITION_KEY_SCANS = 31
      REPLICA_PREFERENCE = 32
      RANDOM_REPLICA = 33
      SCAN_NODE_CODEGEN_THRESHOLD = 34
      DISABLE_STREAMING_PREAGGREGATIONS = 35
      RUNTIME_FILTER_MODE = 36
      RUNTIME_BLOOM_FILTER_SIZE = 37
      RUNTIME_FILTER_WAIT_TIME_MS = 38
      DISABLE_ROW_RUNTIME_FILTERING = 39
      MAX_NUM_RUNTIME_FILTERS = 40
      PARQUET_ANNOTATE_STRINGS_UTF8 = 41
      VALUE_MAP = {0 => "ABORT_ON_ERROR", 1 => "MAX_ERRORS", 2 => "DISABLE_CODEGEN", 3 => "BATCH_SIZE", 4 => "MEM_LIMIT", 5 => "NUM_NODES", 6 => "MAX_SCAN_RANGE_LENGTH", 7 => "MAX_IO_BUFFERS", 8 => "NUM_SCANNER_THREADS", 9 => "ALLOW_UNSUPPORTED_FORMATS", 10 => "DEFAULT_ORDER_BY_LIMIT", 11 => "DEBUG_ACTION", 12 => "ABORT_ON_DEFAULT_LIMIT_EXCEEDED", 13 => "COMPRESSION_CODEC", 14 => "SEQ_COMPRESSION_MODE", 15 => "HBASE_CACHING", 16 => "HBASE_CACHE_BLOCKS", 17 => "PARQUET_FILE_SIZE", 18 => "EXPLAIN_LEVEL", 19 => "SYNC_DDL", 20 => "REQUEST_POOL", 21 => "V_CPU_CORES", 22 => "RESERVATION_REQUEST_TIMEOUT", 23 => "DISABLE_CACHED_READS", 24 => "DISABLE_OUTERMOST_TOPN", 25 => "RM_INITIAL_MEM", 26 => "QUERY_TIMEOUT_S", 27 => "MAX_BLOCK_MGR_MEMORY", 28 => "APPX_COUNT_DISTINCT", 29 => "DISABLE_UNSAFE_SPILLS", 30 => "EXEC_SINGLE_NODE_ROWS_THRESHOLD", 31 => "OPTIMIZE_PARTITION_KEY_SCANS", 32 => "REPLICA_PREFERENCE", 33 => "RANDOM_REPLICA", 34 => "SCAN_NODE_CODEGEN_THRESHOLD", 35 => "DISABLE_STREAMING_PREAGGREGATIONS", 36 => "RUNTIME_FILTER_MODE", 37 => "RUNTIME_BLOOM_FILTER_SIZE", 38 => "RUNTIME_FILTER_WAIT_TIME_MS", 39 => "DISABLE_ROW_RUNTIME_FILTERING", 40 => "MAX_NUM_RUNTIME_FILTERS", 41 => "PARQUET_ANNOTATE_STRINGS_UTF8"}
      VALID_VALUES = Set.new([ABORT_ON_ERROR, MAX_ERRORS, DISABLE_CODEGEN, BATCH_SIZE, MEM_LIMIT, NUM_NODES, MAX_SCAN_RANGE_LENGTH, MAX_IO_BUFFERS, NUM_SCANNER_THREADS, ALLOW_UNSUPPORTED_FORMATS, DEFAULT_ORDER_BY_LIMIT, DEBUG_ACTION, ABORT_ON_DEFAULT_LIMIT_EXCEEDED, COMPRESSION_CODEC, SEQ_COMPRESSION_MODE, HBASE_CACHING, HBASE_CACHE_BLOCKS, PARQUET_FILE_SIZE, EXPLAIN_LEVEL, SYNC_DDL, REQUEST_POOL, V_CPU_CORES, RESERVATION_REQUEST_TIMEOUT, DISABLE_CACHED_READS, DISABLE_OUTERMOST_TOPN, RM_INITIAL_MEM, QUERY_TIMEOUT_S, MAX_BLOCK_MGR_MEMORY, APPX_COUNT_DISTINCT, DISABLE_UNSAFE_SPILLS, EXEC_SINGLE_NODE_ROWS_THRESHOLD, OPTIMIZE_PARTITION_KEY_SCANS, REPLICA_PREFERENCE, RANDOM_REPLICA, SCAN_NODE_CODEGEN_THRESHOLD, DISABLE_STREAMING_PREAGGREGATIONS, RUNTIME_FILTER_MODE, RUNTIME_BLOOM_FILTER_SIZE, RUNTIME_FILTER_WAIT_TIME_MS, DISABLE_ROW_RUNTIME_FILTERING, MAX_NUM_RUNTIME_FILTERS, PARQUET_ANNOTATE_STRINGS_UTF8]).freeze
    end

    class TInsertResult
      include ::Thrift::Struct, ::Thrift::Struct_Union
      ROWS_APPENDED = 1

      FIELDS = {
        ROWS_APPENDED => {:type => ::Thrift::Types::MAP, :name => 'rows_appended', :key => {:type => ::Thrift::Types::STRING}, :value => {:type => ::Thrift::Types::I64}}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field rows_appended is unset!') unless @rows_appended
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TPingImpalaServiceResp
      include ::Thrift::Struct, ::Thrift::Struct_Union
      VERSION = 1

      FIELDS = {
        VERSION => {:type => ::Thrift::Types::STRING, :name => 'version'}
      }

      def struct_fields; FIELDS; end

      def validate
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TResetTableReq
      include ::Thrift::Struct, ::Thrift::Struct_Union
      DB_NAME = 1
      TABLE_NAME = 2

      FIELDS = {
        DB_NAME => {:type => ::Thrift::Types::STRING, :name => 'db_name'},
        TABLE_NAME => {:type => ::Thrift::Types::STRING, :name => 'table_name'}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field db_name is unset!') unless @db_name
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field table_name is unset!') unless @table_name
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TGetExecSummaryReq
      include ::Thrift::Struct, ::Thrift::Struct_Union
      OPERATIONHANDLE = 1
      SESSIONHANDLE = 2

      FIELDS = {
        OPERATIONHANDLE => {:type => ::Thrift::Types::STRUCT, :name => 'operationHandle', :class => ::Impala::Protocol::TOperationHandle, :optional => true},
        SESSIONHANDLE => {:type => ::Thrift::Types::STRUCT, :name => 'sessionHandle', :class => ::Impala::Protocol::TSessionHandle, :optional => true}
      }

      def struct_fields; FIELDS; end

      def validate
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TGetExecSummaryResp
      include ::Thrift::Struct, ::Thrift::Struct_Union
      STATUS = 1
      SUMMARY = 2

      FIELDS = {
        STATUS => {:type => ::Thrift::Types::STRUCT, :name => 'status', :class => ::Impala::Protocol::TStatus},
        SUMMARY => {:type => ::Thrift::Types::STRUCT, :name => 'summary', :class => ::Impala::Protocol::TExecSummary, :optional => true}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field status is unset!') unless @status
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TGetRuntimeProfileReq
      include ::Thrift::Struct, ::Thrift::Struct_Union
      OPERATIONHANDLE = 1
      SESSIONHANDLE = 2

      FIELDS = {
        OPERATIONHANDLE => {:type => ::Thrift::Types::STRUCT, :name => 'operationHandle', :class => ::Impala::Protocol::TOperationHandle, :optional => true},
        SESSIONHANDLE => {:type => ::Thrift::Types::STRUCT, :name => 'sessionHandle', :class => ::Impala::Protocol::TSessionHandle, :optional => true}
      }

      def struct_fields; FIELDS; end

      def validate
      end

      ::Thrift::Struct.generate_accessors self
    end

    class TGetRuntimeProfileResp
      include ::Thrift::Struct, ::Thrift::Struct_Union
      STATUS = 1
      PROFILE = 2

      FIELDS = {
        STATUS => {:type => ::Thrift::Types::STRUCT, :name => 'status', :class => ::Impala::Protocol::TStatus},
        PROFILE => {:type => ::Thrift::Types::STRING, :name => 'profile', :optional => true}
      }

      def struct_fields; FIELDS; end

      def validate
        raise ::Thrift::ProtocolException.new(::Thrift::ProtocolException::UNKNOWN, 'Required field status is unset!') unless @status
      end

      ::Thrift::Struct.generate_accessors self
    end

  end
end

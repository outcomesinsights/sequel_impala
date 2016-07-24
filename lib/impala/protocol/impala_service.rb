#
# Autogenerated by Thrift Compiler (0.9.1)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#

require 'thrift'
require 'beeswax_service'
require 'impala_service_types'

module Impala
  module Protocol
    module ImpalaService
      class Client < ::Impala::Protocol::Beeswax::BeeswaxService::Client 
        include ::Thrift::Client

        def Cancel(query_id)
          send_Cancel(query_id)
          return recv_Cancel()
        end

        def send_Cancel(query_id)
          send_message('Cancel', Cancel_args, :query_id => query_id)
        end

        def recv_Cancel()
          result = receive_message(Cancel_result)
          return result.success unless result.success.nil?
          raise result.error unless result.error.nil?
          raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'Cancel failed: unknown result')
        end

        def ResetCatalog()
          send_ResetCatalog()
          return recv_ResetCatalog()
        end

        def send_ResetCatalog()
          send_message('ResetCatalog', ResetCatalog_args)
        end

        def recv_ResetCatalog()
          result = receive_message(ResetCatalog_result)
          return result.success unless result.success.nil?
          raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'ResetCatalog failed: unknown result')
        end

        def ResetTable(request)
          send_ResetTable(request)
          return recv_ResetTable()
        end

        def send_ResetTable(request)
          send_message('ResetTable', ResetTable_args, :request => request)
        end

        def recv_ResetTable()
          result = receive_message(ResetTable_result)
          return result.success unless result.success.nil?
          raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'ResetTable failed: unknown result')
        end

        def GetRuntimeProfile(query_id)
          send_GetRuntimeProfile(query_id)
          return recv_GetRuntimeProfile()
        end

        def send_GetRuntimeProfile(query_id)
          send_message('GetRuntimeProfile', GetRuntimeProfile_args, :query_id => query_id)
        end

        def recv_GetRuntimeProfile()
          result = receive_message(GetRuntimeProfile_result)
          return result.success unless result.success.nil?
          raise result.error unless result.error.nil?
          raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'GetRuntimeProfile failed: unknown result')
        end

        def CloseInsert(handle)
          send_CloseInsert(handle)
          return recv_CloseInsert()
        end

        def send_CloseInsert(handle)
          send_message('CloseInsert', CloseInsert_args, :handle => handle)
        end

        def recv_CloseInsert()
          result = receive_message(CloseInsert_result)
          return result.success unless result.success.nil?
          raise result.error unless result.error.nil?
          raise result.error2 unless result.error2.nil?
          raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'CloseInsert failed: unknown result')
        end

        def PingImpalaService()
          send_PingImpalaService()
          return recv_PingImpalaService()
        end

        def send_PingImpalaService()
          send_message('PingImpalaService', PingImpalaService_args)
        end

        def recv_PingImpalaService()
          result = receive_message(PingImpalaService_result)
          return result.success unless result.success.nil?
          raise ::Thrift::ApplicationException.new(::Thrift::ApplicationException::MISSING_RESULT, 'PingImpalaService failed: unknown result')
        end

      end

      class Processor < ::Impala::Protocol::Beeswax::BeeswaxService::Processor 
        include ::Thrift::Processor

        def process_Cancel(seqid, iprot, oprot)
          args = read_args(iprot, Cancel_args)
          result = Cancel_result.new()
          begin
            result.success = @handler.Cancel(args.query_id)
          rescue ::Impala::Protocol::Beeswax::BeeswaxException => error
            result.error = error
          end
          write_result(result, oprot, 'Cancel', seqid)
        end

        def process_ResetCatalog(seqid, iprot, oprot)
          args = read_args(iprot, ResetCatalog_args)
          result = ResetCatalog_result.new()
          result.success = @handler.ResetCatalog()
          write_result(result, oprot, 'ResetCatalog', seqid)
        end

        def process_ResetTable(seqid, iprot, oprot)
          args = read_args(iprot, ResetTable_args)
          result = ResetTable_result.new()
          result.success = @handler.ResetTable(args.request)
          write_result(result, oprot, 'ResetTable', seqid)
        end

        def process_GetRuntimeProfile(seqid, iprot, oprot)
          args = read_args(iprot, GetRuntimeProfile_args)
          result = GetRuntimeProfile_result.new()
          begin
            result.success = @handler.GetRuntimeProfile(args.query_id)
          rescue ::Impala::Protocol::Beeswax::BeeswaxException => error
            result.error = error
          end
          write_result(result, oprot, 'GetRuntimeProfile', seqid)
        end

        def process_CloseInsert(seqid, iprot, oprot)
          args = read_args(iprot, CloseInsert_args)
          result = CloseInsert_result.new()
          begin
            result.success = @handler.CloseInsert(args.handle)
          rescue ::Impala::Protocol::Beeswax::QueryNotFoundException => error
            result.error = error
          rescue ::Impala::Protocol::Beeswax::BeeswaxException => error2
            result.error2 = error2
          end
          write_result(result, oprot, 'CloseInsert', seqid)
        end

        def process_PingImpalaService(seqid, iprot, oprot)
          args = read_args(iprot, PingImpalaService_args)
          result = PingImpalaService_result.new()
          result.success = @handler.PingImpalaService()
          write_result(result, oprot, 'PingImpalaService', seqid)
        end

      end

      # HELPER FUNCTIONS AND STRUCTURES

      class Cancel_args
        include ::Thrift::Struct, ::Thrift::Struct_Union
        QUERY_ID = 1

        FIELDS = {
          QUERY_ID => {:type => ::Thrift::Types::STRUCT, :name => 'query_id', :class => ::Impala::Protocol::Beeswax::QueryHandle}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class Cancel_result
        include ::Thrift::Struct, ::Thrift::Struct_Union
        SUCCESS = 0
        ERROR = 1

        FIELDS = {
          SUCCESS => {:type => ::Thrift::Types::STRUCT, :name => 'success', :class => ::Impala::Protocol::TStatus},
          ERROR => {:type => ::Thrift::Types::STRUCT, :name => 'error', :class => ::Impala::Protocol::Beeswax::BeeswaxException}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class ResetCatalog_args
        include ::Thrift::Struct, ::Thrift::Struct_Union

        FIELDS = {

        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class ResetCatalog_result
        include ::Thrift::Struct, ::Thrift::Struct_Union
        SUCCESS = 0

        FIELDS = {
          SUCCESS => {:type => ::Thrift::Types::STRUCT, :name => 'success', :class => ::Impala::Protocol::TStatus}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class ResetTable_args
        include ::Thrift::Struct, ::Thrift::Struct_Union
        REQUEST = 1

        FIELDS = {
          REQUEST => {:type => ::Thrift::Types::STRUCT, :name => 'request', :class => ::Impala::Protocol::TResetTableReq}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class ResetTable_result
        include ::Thrift::Struct, ::Thrift::Struct_Union
        SUCCESS = 0

        FIELDS = {
          SUCCESS => {:type => ::Thrift::Types::STRUCT, :name => 'success', :class => ::Impala::Protocol::TStatus}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class GetRuntimeProfile_args
        include ::Thrift::Struct, ::Thrift::Struct_Union
        QUERY_ID = 1

        FIELDS = {
          QUERY_ID => {:type => ::Thrift::Types::STRUCT, :name => 'query_id', :class => ::Impala::Protocol::Beeswax::QueryHandle}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class GetRuntimeProfile_result
        include ::Thrift::Struct, ::Thrift::Struct_Union
        SUCCESS = 0
        ERROR = 1

        FIELDS = {
          SUCCESS => {:type => ::Thrift::Types::STRING, :name => 'success'},
          ERROR => {:type => ::Thrift::Types::STRUCT, :name => 'error', :class => ::Impala::Protocol::Beeswax::BeeswaxException}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class CloseInsert_args
        include ::Thrift::Struct, ::Thrift::Struct_Union
        HANDLE = 1

        FIELDS = {
          HANDLE => {:type => ::Thrift::Types::STRUCT, :name => 'handle', :class => ::Impala::Protocol::Beeswax::QueryHandle}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class CloseInsert_result
        include ::Thrift::Struct, ::Thrift::Struct_Union
        SUCCESS = 0
        ERROR = 1
        ERROR2 = 2

        FIELDS = {
          SUCCESS => {:type => ::Thrift::Types::STRUCT, :name => 'success', :class => ::Impala::Protocol::TInsertResult},
          ERROR => {:type => ::Thrift::Types::STRUCT, :name => 'error', :class => ::Impala::Protocol::Beeswax::QueryNotFoundException},
          ERROR2 => {:type => ::Thrift::Types::STRUCT, :name => 'error2', :class => ::Impala::Protocol::Beeswax::BeeswaxException}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class PingImpalaService_args
        include ::Thrift::Struct, ::Thrift::Struct_Union

        FIELDS = {

        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

      class PingImpalaService_result
        include ::Thrift::Struct, ::Thrift::Struct_Union
        SUCCESS = 0

        FIELDS = {
          SUCCESS => {:type => ::Thrift::Types::STRUCT, :name => 'success', :class => ::Impala::Protocol::TPingImpalaServiceResp}
        }

        def struct_fields; FIELDS; end

        def validate
        end

        ::Thrift::Struct.generate_accessors self
      end

    end

  end
end

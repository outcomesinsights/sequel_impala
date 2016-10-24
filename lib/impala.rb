
# the generated ruby files use a relative require, so we need to add the
# generated directory to $LOAD_PATH
this_dir = File.expand_path(File.dirname(__FILE__))
gen_dir = File.join(this_dir, 'impala/protocol')
$LOAD_PATH.push(gen_dir) unless $LOAD_PATH.include?(gen_dir)

require 'impala/version'

require 'thrift'
require 'time'
require 'impala/protocol'
begin
require 'impala/sasl_transport'
rescue LoadError
  # gssapi not supported by operating system, continue as impala adapter
  # can be used in buffered (non-SASL) mode.
end
require 'impala/progress_reporter'
require 'impala/cursor'
require 'impala/connection'
require 'impala/thrift_patch'

module Impala
  DEFAULT_HOST = 'localhost'
  DEFAULT_PORT = 21000
  class InvalidQueryError < StandardError; end
  class ConnectionError < StandardError; end
  class CursorError < StandardError; end
  class ParsingError < StandardError; end

  # Connect to an Impala server. If a block is given, it will close the
  # connection after yielding the connection to the block.
  # @param [String] host the hostname or IP address of the Impala server
  # @param [int] port the port that the Impala server is listening on
  # @yieldparam [Connection] conn the open connection. Will be closed once the block
  #    finishes
  # @return [Connection] the open connection, or, if a block is
  #    passed, the return value of the block
  def self.connect(host=DEFAULT_HOST, port=DEFAULT_PORT, options={})
    connection = Connection.new(host, port, options)

    if block_given?
      begin
        ret = yield connection
      ensure
        connection.close
      end
    else
      ret = connection
    end

    ret
  end
end

require 'socket'

module Thrift
  module KeepAlive
    # We'll override #open so that once the socket is opened
    # we enable keepalive on it
    #
    # Many queries are going to take a long time (10s of minutes) to complete
    # and we don't want the connection to close while we wait for the
    # query to return.
    #
    # Unfortunately, Thrift doesn't supply an easy way to get to the
    # socket that it opens to communicate with Impala.
    #
    # I figured that while I was in here, monkey-patching a way to get
    # to the socket, I might as well just enable keepalive here
    # instead.
    def open
      super
      yield @transport if block_given?
    end
  end

  class BufferedTransport
    prepend KeepAlive
  end

  class ImpalaSaslClientTransport
    prepend KeepAlive
  end
end

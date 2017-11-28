warn 'jdbc-impala is only for use with JRuby' if (JRUBY_VERSION.nil? rescue true)

module Jdbc
  module Impala
    DRIVER_VERSION = '2.5.41.1061'
    VERSION = DRIVER_VERSION
    JAR_ROOT = ENV['IMPALA_JDBC_JARS']
    unless JAR_ROOT && File.directory?(JAR_ROOT)
      warn "must specify IMPALA_JDBC_JARS environment variable for directory containing necessary jar files for Impala JDBC 4.1 driver version #{VERSION}"
      raise LoadError, "cannot load such file -- jdbc/impala"
    end

    def self.driver_jar
      %W(
        ImpalaJDBC41.jar
        TCLIServiceClient.jar
        commons-codec-1.3.jar
        commons-logging-1.1.1.jar
        hive_metastore.jar
        hive_service.jar
        httpclient-4.1.3.jar
        httpcore-4.1.3.jar
        libfb303-0.9.0.jar
        libthrift-0.9.0.jar
        log4j-1.2.14.jar
        ql.jar
        slf4j-api-1.5.11.jar
        slf4j-log4j12-1.5.11.jar
        zookeeper-3.4.6.jar
      ).map{|f| File.join(JAR_ROOT, f)}
    end

    def self.load_driver(method = :load)
      driver_jar.each do |jar|
        send method, jar
      end
    end

    def self.driver_name
      'com.cloudera.impala.jdbc41.Driver'
    end

    if defined?(JRUBY_VERSION) && # enable backwards-compat behavior
      (Java::JavaLang::Boolean.get_boolean('jdbc.driver.autoload'))
      warn "autoloading jdbc driver on require 'jdbc/impala'" if $VERBOSE
      load_driver :require
    end
  end
end


require "rake"
require "rake/clean"

CLEAN.include ["sequel_impala-*.gem", "rdoc"]

desc "Build sequel_impala gem"
task :package=>[:clean] do |p|
  sh %{#{FileUtils::RUBY} -S gem build sequel_impala.gemspec}
end

### Specs

desc "Run specs"
task "spec" do
  sh "#{FileUtils::RUBY} -rubygems -I lib -e 'ARGV.each{|f| require f}' ./spec/*_test.rb"
end

task :default => :spec

### RDoc

RDOC_DEFAULT_OPTS = ["--quiet", "--line-numbers", "--inline-source", '--title', 'sequel_impala: Sequel support for Impala database']

begin
  gem 'rdoc'
  gem 'hanna-nouveau'
  RDOC_DEFAULT_OPTS.concat(['-f', 'hanna'])
rescue Gem::LoadError
end

RDOC_OPTS = RDOC_DEFAULT_OPTS + ['--main', 'README.rdoc']

require 'rdoc/task'
RDoc::Task.new do |rdoc|
  rdoc.rdoc_dir = "rdoc"
  rdoc.options += RDOC_OPTS
  rdoc.rdoc_files.add %w"README.rdoc CHANGELOG LICENSE lib/**/*.rb"
end


require "rake"
require "rake/clean"
require "rake/testtask"

CLEAN.include ["sequel_impala-*.gem", "rdoc"]

desc "Build sequel_impala gem"
task :package=>[:clean] do |p|
  sh %{#{FileUtils::RUBY} -S gem build sequel_impala.gemspec}
end

Rake::TestTask.new do |t|
  t.libs << "."
  t.libs << "spec"
  t.test_files = FileList['spec/**/*_test.rb']
  t.warning = false
end

task :default => :test

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


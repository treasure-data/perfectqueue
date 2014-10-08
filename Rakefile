require 'rake'
require 'rake/testtask'
require 'rake/clean'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "perfectqueue"
    gemspec.summary = "Highly available distributed queue built on RDBMS or SimpleDB"
    gemspec.author = "Sadayuki Furuhashi"
    gemspec.email = "frsyuki@gmail.com"
    gemspec.homepage = "https://github.com/treasure-data/perfectqueue"
    #gemspec.has_rdoc = false
    gemspec.require_paths = ["lib"]
    gemspec.add_dependency "sequel", "~> 3.48.0"
    gemspec.test_files = Dir["test/**/*.rb", "test/**/*.sh"]
    gemspec.files = Dir["bin/**/*", "lib/**/*"]
    gemspec.executables = ['perfectqueue']
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end

Rake::TestTask.new(:test) do |t|
  t.test_files = Dir['test/*_test.rb']
  t.ruby_opts = ['-rubygems'] if defined? Gem
  t.ruby_opts << '-I.'
end

VERSION_FILE = "lib/perfectqueue/version.rb"

file VERSION_FILE => ["VERSION"] do |t|
  version = File.read("VERSION").strip
  File.open(VERSION_FILE, "w") {|f|
    f.write <<EOF
module PerfectQueue

VERSION = '#{version}'

end
EOF
  }
end

task :default => [VERSION_FILE, :build]


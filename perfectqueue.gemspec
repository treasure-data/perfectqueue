# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)
require 'perfectqueue/version'

Gem::Specification.new do |gem|
  gem.name        = "perfectqueue"
  gem.description = "Highly available distributed cron built on RDBMS"
  gem.homepage    = "https://github.com/treasure-data/perfectqueue"
  gem.summary     = gem.description
  gem.version     = PerfectQueue::VERSION
  gem.authors     = ["Sadayuki Furuhashi"]
  gem.email       = "frsyuki@gmail.com"
  gem.has_rdoc    = false
  gem.files       = `git ls-files`.split("\n")
  gem.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ['lib']

  gem.add_dependency "sequel", "~> 3.26.0"
  gem.add_development_dependency "rake", "~> 0.9.2"
  gem.add_development_dependency "rspec", "~> 2.8.0"
  gem.add_development_dependency "simplecov", "~> 0.5.4"
  gem.add_development_dependency "sqlite3", "~> 1.3.3"
end

$LOAD_PATH.unshift(File.expand_path('../lib', File.dirname(__FILE__)))

require 'perfectqueue'

if ENV['SIMPLE_COV']
  require 'simplecov'
  SimpleCov.start do
    add_filter 'spec/'
    add_filter 'pkg/'
    add_filter 'vendor/'
  end
end

require 'fileutils'

module QueueTest
  def self.included(mod)
    mod.module_eval do
      let :database_path do
        'spec/test.db'
      end

      let :queue_config do
        {
          :type => 'rdb_compat',
          :url => "sqlite://#{database_path}",
          #:url => "mysql2://root:@localhost/test",
          :table => 'test_tasks',
          :processor_type => 'thread',
          :cleanup_interval => 0,  # for test
          #:disable_resource_limit => true,  # TODO backend-specific test cases
        }
      end

      let :queue do
        PerfectQueue.open(queue_config)
      end

      before do
        FileUtils.rm_f database_path
        #queue.client.backend.instance_variable_get(:@db).run 'DROP TABLE IF EXISTS `test_tasks`'
        queue.client.init_database
      end

      after do
        queue.close
      end
    end
  end
end


include PerfectQueue


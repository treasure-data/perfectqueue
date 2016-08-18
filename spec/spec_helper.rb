$LOAD_PATH.unshift(File.expand_path('../lib', File.dirname(__FILE__)))

if ENV['SIMPLE_COV']
  require 'simplecov'
  SimpleCov.start do
    add_filter 'spec/'
    add_filter 'pkg/'
    add_filter 'vendor/'
  end
end

require 'perfectqueue'

if ENV["CI"]
  require 'coveralls'
  Coveralls.wear!
end

require 'fileutils'

module QueueTest
  def self.included(mod)
    mod.module_eval do
      let :queue_config do
        {
          :type => 'rdb_compat',
          :url => "mysql2://root:@localhost/perfectqueue_test",
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
        queue.client.init_database(:force => true)
      end

      after do
        queue.close
      end
    end
  end
end


include PerfectQueue


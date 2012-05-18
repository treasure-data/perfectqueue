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

def test_queue_config
  {:type=>'rdb_compat', :url=>'sqlite://spec/test.db', :table=>'test_tasks'}
end

def create_test_queue
  FileUtils.rm_f 'spec/test.db'
  queue = PerfectQueue.open(test_queue_config)

  sql = %[
      CREATE TABLE IF NOT EXISTS `test_tasks` (
        id VARCHAR(256) NOT NULL,
        timeout INT NOT NULL,
        data BLOB NOT NULL,
        created_at INT,
        resource VARCHAR(256),
        PRIMARY KEY (id)
      );]

  queue.client.backend.db.run sql

  return queue
end

def get_test_queue
  PerfectQueue.open(test_queue_config)
end

include PerfectQueue


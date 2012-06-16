require 'spec_helper'
require 'perfectqueue/backend/rdb_compat'

describe Backend::RDBCompatBackend do
  let :queue do
    FileUtils.rm_f 'spec/test.db'
    queue = PerfectQueue.open({:type=>'rdb_compat', :url=>'sqlite://spec/test.db', :table=>'test_tasks', :processor_type=>'thread'})
    queue.client.init_database
    queue
  end

  let :client do
    queue.client
  end

  let :backend do
    client.backend
  end

  it 'backward compatibility 1' do
    backend.db["INSERT INTO test_tasks (id, timeout, data, created_at, resource) VALUES (?, ?, ?, ?, ?)", "merge_type.1339801200", 1339801201, {'url'=>nil}.to_json, 1339801201, "1"].insert
    ts = backend.acquire(60, 1, {:now=>1339801203})
    ts.should_not == nil
    t = ts[0]
    t.data.should == {'url'=>nil}
    t.type.should == 'merge_type'
    t.key.should == 'merge_type.1339801200'
  end

  it 'backward compatibility 2' do
    backend.db["INSERT INTO test_tasks (id, timeout, data, created_at, resource) VALUES (?, ?, ?, ?, ?)", "query.379474", 1339801201, {'query_id'=>32}.to_json, 1339801201, nil].insert
    ts = backend.acquire(60, 1, {:now=>1339801203})
    ts.should_not == nil
    t = ts[0]
    t.data.should == {'query_id'=>32}
    t.type.should == 'query'
    t.key.should == 'query.379474'
  end
end


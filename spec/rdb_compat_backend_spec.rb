require 'spec_helper'
require 'perfectqueue/backend/rdb_compat'

describe Backend::RDBCompatBackend do
  include QueueTest

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

  it 'resource limit' do
    time = Time.now.to_i

    3.times do |i|
      queue.submit("test_#{i}", 'user01', {}, :now=>time-(i+1), :user=>'u1', :max_running=>2)
    end
    queue.submit("test_5", 'user02', {}, :now=>time, :user=>'u2', :max_running=>2)

    task1 = queue.poll(:now=>time+10)
    task1.should_not == nil
    task1.type.should == 'user01'

    task2 = queue.poll(:now=>time+10)
    task2.should_not == nil
    task2.type.should == 'user02'

    task3 = queue.poll(:now=>time+10)
    task3.should_not == nil
    task3.type.should == 'user01'

    task4 = queue.poll(:now=>time+10)
    task4.should == nil

    task1.finish!

    task5 = queue.poll(:now=>time+10)
    task5.should_not == nil
    task5.type.should == 'user01'
  end

  it 'gzip data compression' do
    time = Time.now.to_i
    queue.submit("test", 'user01', {'data'=>'test'}, :now=>time, :user=>'u1', :max_running=>2, :compression=>'gzip')

    task1 = queue.poll(:now=>time+10)
    task1.should_not == nil
    task1.data.should == {'data'=>'test'}
  end
end


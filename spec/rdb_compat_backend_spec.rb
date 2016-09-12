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
    expect(ts).not_to eq(nil)
    t = ts[0]
    expect(t.data).to eq({'url'=>nil})
    expect(t.type).to eq('merge_type')
    expect(t.key).to eq('merge_type.1339801200')
  end

  it 'backward compatibility 2' do
    backend.db["INSERT INTO test_tasks (id, timeout, data, created_at, resource) VALUES (?, ?, ?, ?, ?)", "query.379474", 1339801201, {'query_id'=>32}.to_json, 1339801201, nil].insert
    ts = backend.acquire(60, 1, {:now=>1339801203})
    expect(ts).not_to eq(nil)
    t = ts[0]
    expect(t.data).to eq({'query_id'=>32})
    expect(t.type).to eq('query')
    expect(t.key).to eq('query.379474')
  end

  it 'resource limit' do
    time = Time.now.to_i

    3.times do |i|
      queue.submit("test_#{i}", 'user01', {}, :now=>time-(i+1), :user=>'u1', :max_running=>2)
    end
    queue.submit("test_5", 'user02', {}, :now=>time, :user=>'u2', :max_running=>2)

    task1 = queue.poll(:now=>time+10)
    expect(task1).not_to eq(nil)
    expect(task1.type).to eq('user01')

    task2 = queue.poll(:now=>time+10)
    expect(task2).not_to eq(nil)
    expect(task2.type).to eq('user02')

    task3 = queue.poll(:now=>time+10)
    expect(task3).not_to eq(nil)
    expect(task3.type).to eq('user01')

    task4 = queue.poll(:now=>time+10)
    expect(task4).to eq(nil)

    task1.finish!

    task5 = queue.poll(:now=>time+10)
    expect(task5).not_to eq(nil)
    expect(task5.type).to eq('user01')
  end

  it 'gzip data compression' do
    time = Time.now.to_i
    queue.submit("test", 'user01', {'data'=>'test'}, :now=>time, :user=>'u1', :max_running=>2, :compression=>'gzip')

    task1 = queue.poll(:now=>time+10)
    expect(task1).not_to eq(nil)
    expect(task1.data).to eq({'data'=>'test'})
  end
end

describe Backend::RDBCompatBackend do
  let (:now){ Time.now.to_i }
  let (:client){ double('client') }
  let (:table){ 'test_queues' }
  let (:config){ {url: 'mysql2://root:@localhost/perfectqueue_test', table: table} }
  let (:db) do
    d = Backend::RDBCompatBackend.new(client, config)
    s = d.db
    s.tables.each{|t| s.drop_table(t) }
    d.init_database({})
    d
  end

  context '.new' do
    let (:client){ double('client') }
    let (:table){ double('table') }
    it 'raises error unless url' do
      expect{Backend::RDBCompatBackend.new(client, {})}.to raise_error(ConfigError)
    end
    it 'raises error unless table' do
      expect{Backend::RDBCompatBackend.new(client, {url: ''})}.to raise_error(ConfigError)
    end
    it 'supports mysql' do
      expect(Backend::RDBCompatBackend.new(client, config)).to be_an_instance_of(Backend::RDBCompatBackend)
      expect(db.instance_variable_get(:@sql)).to include('max_running')
    end
    it 'doesn\'t support postgres' do
      config = {url: 'postgres://localhost', table: table}
      expect{Backend::RDBCompatBackend.new(client, config)}.to raise_error(ConfigError)
    end
    it 'with use_connection_pooling' do
      config = {url: 'mysql2://root:@localhost/perfectqueue_test', table: table, use_connection_pooling: true}
      db = Backend::RDBCompatBackend.new(client, config)
      expect(db.instance_variable_get(:@use_connection_pooling)).to eq true
    end
    it 'disable_resource_limit' do
      config = {url: 'mysql2://root:@localhost/perfectqueue_test', table: table, disable_resource_limit: true}
      db = Backend::RDBCompatBackend.new(client, config)
      expect(db.instance_variable_get(:@sql)).not_to include('max_running')
    end
  end

  context '#init_database' do
    let (:db) do
      d = Backend::RDBCompatBackend.new(client, config)
      s = d.db
      s.tables.each{|t| s.drop_table(t) }
      d
    end
    it 'creates the table' do
      db.init_database({})
    end
    it 'raises DatabaseError if already exists' do
      expect(STDERR).to receive(:puts)
      db.init_database({})
      expect{db.init_database({})}.to raise_error(Sequel::DatabaseError)
    end
    it 'drops the table if force: true' do
      db.init_database({})
      db.init_database({force: true})
    end
  end

  context '#get_task_metadata' do
    before do
      db.submit('key', 'test', nil, {})
    end
    it 'fetches a metadata' do
      expect(db.get_task_metadata('key', {})).to be_an_instance_of(TaskMetadata)
    end
    it 'raises error if non exist key' do
      expect(STDERR).to receive(:puts)
      expect{db.get_task_metadata('nonexistent', {})}.to raise_error(NotFoundError)
    end
  end

  context '#preempt' do
    subject { db.preempt(nil, nil, nil) }
    it { expect{ subject }.to raise_error(NotSupportedError) }
  end

  context '#list' do
    before do
      db.submit('key', 'test', nil, {})
    end
    it 'lists a metadata' do
      db.list({}) do |x|
        expect(x).to be_an_instance_of(TaskWithMetadata)
        expect(x.key).to eq('key')
      end
    end
  end

  context '#submit' do
    it 'returns true' do
      expect(db.submit('key', 'test', nil, {})).to be_an_instance_of(Task)
    end
    it 'returns true (gzip)' do
      expect(db.submit('key', 'test', nil, {compression: 'gzip'})).to be_an_instance_of(Task)
    end
    it 'returns nil if duplication' do
      expect(db.submit('key', 'test', nil, {})).to be_an_instance_of(Task)
      expect{db.submit('key', 'test', nil, {})}.to raise_error(IdempotentAlreadyExistsError)
    end
  end

  context '#acquire' do
    let (:key){ 'key' }
    let (:task_token){ Backend::RDBCompatBackend::Token.new(key) }
    let (:alive_time){ 42 }
    let (:max_acquire){ 42 }
    context 'no tasks' do
      it 'returns nil' do
        expect(db.acquire(alive_time, max_acquire, {})).to be_nil
      end
    end
    context 'some tasks' do
      before do
        db.submit(key, 'test', nil, {})
      end
      it 'returns a task' do
        ary = db.acquire(alive_time, max_acquire, {})
        expect(ary).to be_an_instance_of(Array)
        expect(ary.size).to eq(1)
        expect(ary[0]).to be_an_instance_of(AcquiredTask)
      end
    end
    context 'disable_resource_limit' do
      let (:config) do
        {url: 'mysql2://root:@localhost/perfectqueue_test', table: table, disable_resource_limit: true}
      end
      before do
        db.submit(key, 'test', nil, {})
      end
      it 'returns a task' do
        ary = db.acquire(alive_time, max_acquire, {})
        expect(ary).to be_an_instance_of(Array)
        expect(ary.size).to eq(1)
        expect(ary[0]).to be_an_instance_of(AcquiredTask)
      end
    end
    context 'some tasks' do
      let :t0 do now - 300 end
      let :t1 do now - 200 end
      let :t2 do now - 100 end
      before do
        db.submit('key1', 'test1', nil, {now: t0})
        db.submit('key2', 'test2', nil, {now: t0})
        db.submit('key3', 'test3', nil, {now: t1})
        db.submit('key4', 'test4', nil, {now: t2})
        db.submit('key5', 'test5', nil, {now: t2})
      end
      it 'returns 5 tasks' do
        ary = []
        db.list({}){|task| ary << task }
        expect(ary[0].timeout.to_i).to eq t0
        expect(ary[1].timeout.to_i).to eq t0
        expect(ary[2].timeout.to_i).to eq t1
        expect(ary[3].timeout.to_i).to eq t2
        expect(ary[4].timeout.to_i).to eq t2

        ary = db.acquire(alive_time, max_acquire, {now: now})
        expect(ary).to be_an_instance_of(Array)
        expect(ary.size).to eq(5)
        expect(ary[0]).to be_an_instance_of(AcquiredTask)
        expect(ary[1]).to be_an_instance_of(AcquiredTask)
        expect(ary[2]).to be_an_instance_of(AcquiredTask)
        expect(ary[3]).to be_an_instance_of(AcquiredTask)
        expect(ary[4]).to be_an_instance_of(AcquiredTask)

        now1 = Time.at(now + alive_time)
        expect(now1).to receive(:to_time).exactly(5).times.and_call_original
        db.list({}){|task| expect(task.timeout).to eq now1.to_time }
      end
    end
  end

  context '#force_finish' do
    let (:key){ double('key') }
    let (:token){ double('token') }
    let (:retention_time){ double('retention_time') }
    let (:options){ double('options') }
    let (:ret){ double('ret') }
    before { expect(Backend::RDBCompatBackend::Token).to receive(:new).with(key).and_return(token) }
    it 'calls #finish' do
      expect(db).to receive(:finish).with(token, retention_time, options).exactly(:once).and_return(ret)
      expect(db.force_finish(key, retention_time, options)).to eq ret
    end
  end

  context '#finish' do
    let (:key){ 'key' }
    let (:task_token){ Backend::RDBCompatBackend::Token.new(key) }
    let (:retention_time) { 42 }
    let (:delete_timeout){ now - Backend::RDBCompatBackend::DELETE_OFFSET + retention_time }
    let (:options){ {now: now} }
    context 'have the task' do
      before do
        db.submit(key, 'test', nil, {})
        expect(db.db).to receive(:[]).with(kind_of(String), delete_timeout, key).and_call_original
      end
      it 'returns nil' do
        expect(db.finish(task_token, retention_time, options)).to be_nil
        row = db.db.fetch("SELECT created_at FROM `#{table}` WHERE id=? LIMIT 1", key).first
        expect(row[:created_at]).to be_nil
      end
    end
    context 'already finished' do
      it 'raises IdempotentAlreadyFinishedError' do
        expect(STDERR).to receive(:puts)
        expect{db.finish(task_token, retention_time, options)}.to raise_error(IdempotentAlreadyFinishedError)
      end
    end
  end

  context '#heartbeat' do
    let (:key){ 'key' }
    let (:task_token){ Backend::RDBCompatBackend::Token.new(key) }
    let (:retention_time) { 42 }
    let (:delete_timeout){ now + retention_time }
    let (:options){ {now: now} }
    before{ allow(STDERR).to receive(:puts) }
    context 'have a queueuled task' do
      before do
        db.submit(key, 'test', nil, {})
      end
      it 'returns nil if next_run_time is not updated' do
        expect(db.heartbeat(task_token, 0, {now: now})).to be_nil
      end
      it 'returns nil even if next_run_time is updated' do
        expect(db.heartbeat(task_token, 1, {})).to be_nil
      end
    end
    context 'no tasks' do
      it 'raises PreemptedError' do
        expect{db.heartbeat(task_token, 0, {})}.to raise_error(PreemptedError)
      end
    end
    context 'finished task' do
      before do
        db.submit(key, 'test', nil, {})
        db.finish(task_token, retention_time, options)
      end
      it 'raises PreemptedError' do
        expect{db.heartbeat(task_token, 0, {})}.to raise_error(PreemptedError)
      end
    end
  end

  context '#connect' do
    context 'normal' do
      it 'returns now' do
        expect(db.__send__(:connect){ }).to eq(now)
      end
    end
    context 'error' do
      it 'returns block result' do
        expect(RuntimeError).to receive(:new).exactly(Backend::RDBCompatBackend::MAX_RETRY).and_call_original
        allow(STDERR).to receive(:puts)
        allow(db).to receive(:sleep)
        expect do
          db.__send__(:connect) do
            raise RuntimeError.new('try restarting transaction')
          end
        end.to raise_error(RuntimeError)
      end
    end
  end

  context '#create_attributes' do
    let (:data){ Hash.new }
    let (:row) do
      r = double('row')
      allow(r).to receive(:[]){|k| data[k] }
      r
    end
    it 'returns a hash consisting the data of the row' do
      data[:timezone] = timezone = double('timezone')
      data[:delay] = delay = double('delay')
      data[:cron] = cron = double('cron')
      data[:next_time] = next_time = double('next_time')
      data[:timeout] = timeout = double('timeout')
      data[:data] = '{"type":"foo.bar","a":"b"}'
      data[:id] = 'hoge'
      expect(db.__send__(:create_attributes, now, row)).to eq(
        status: :finished,
        created_at: nil,
        data: {"a"=>"b"},
        user: nil,
        timeout: timeout,
        max_running: nil,
        type: 'foo.bar',
        message: nil,
        node: nil,
        compression: nil,
      )
    end
    it 'returns {} if data\'s JSON is broken' do
      data[:data] = '}{'
      data[:id] = 'foo.bar.baz'
      expect(db.__send__(:create_attributes, now, row)).to eq(
        status: :finished,
        created_at: nil,
        data: {},
        user: nil,
        timeout: nil,
        max_running: nil,
        type: 'foo',
        message: nil,
        node: nil,
        compression: nil,
      )
    end
    it 'uses id[/\A[^.]*/] if type is empty string' do
      data[:data] = '{"type":""}'
      data[:id] = 'foo.bar.baz'
      expect(db.__send__(:create_attributes, now, row)).to eq(
        status: :finished,
        created_at: nil,
        data: {},
        user: nil,
        timeout: nil,
        max_running: nil,
        type: 'foo',
        message: nil,
        node: nil,
        compression: nil,
      )
    end
    it 'uses id[/\A[^.]*/] if type is nil' do
      data[:id] = 'foo.bar.baz'
      expect(db.__send__(:create_attributes, now, row)).to eq(
        status: :finished,
        created_at: nil,
        data: {},
        user: nil,
        timeout: nil,
        max_running: nil,
        type: 'foo',
        message: nil,
        node: nil,
        compression: nil,
      )
    end
  end

  context '#connect_locked' do
    let (:ret){ double('ret') }
    before do
    end
    it 'ensures to unlock on error with use_connection_pooling' do
      #expect(STDERR).to receive(:puts)
      config = {url: 'mysql2://root:@localhost/perfectqueue_test', table: table, use_connection_pooling: true}
      db1 = Backend::RDBCompatBackend.new(client, config)
      #expect{ db.__send__(:connect_locked){ raise } }.to raise_error(RuntimeError)
      db1.__send__(:connect_locked){ ret }
      stub_const('PerfectQueue::Backend::RDBCompatBackend::LOCK_WAIT_TIMEOUT', 5)
      db2 = Backend::RDBCompatBackend.new(client, config)
      Timeout.timeout(3) do
        expect( db2.__send__(:connect_locked){ ret }).to eq ret
      end
    end
  end

  context '#create_attributes' do
    let (:data){ {data: '{"type":"foo"}'} }
    let (:timeout){ double('timeout') }
    let (:row) do
      r = double('row')
      allow(r).to receive(:[]){|k| data[k] }
      r
    end
    context 'created_at is nil' do
      it 'returns a hash consisting the data of the row' do
        data[:resource] = user = double('user')
        data[:max_running] = max_running = double('max_running')
        data[:cron] = cron = double('cron')
        data[:next_time] = next_time = double('next_time')
        data[:timeout] = timeout
        data[:data] = '{"type":"foo.bar","a":"b"}'
        data[:id] = 'hoge'
        expect(db.__send__(:create_attributes, now, row)).to eq(
          status: TaskStatus::FINISHED,
          created_at: nil,
          data: {"a"=>"b"},
          type: 'foo.bar',
          user: user,
          timeout: timeout,
          max_running: max_running,
          message: nil,
          node: nil,
          compression: nil,
        )
      end
      it 'returns {} if data\'s JSON is broken' do
        data[:data] = '}{'
        data[:id] = 'foo.bar.baz'
        r = db.__send__(:create_attributes, now, row)
        expect(r[:type]).to eq 'foo'
      end
      it 'uses id[/\A[^.]*/] if type is empty string' do
        data[:data] = '{"type":""}'
        data[:id] = 'foo.bar.baz'
        r = db.__send__(:create_attributes, now, row)
        expect(r[:type]).to eq 'foo'
      end
      it 'uses id[/\A[^.]*/] if type is nil' do
        data[:id] = 'foo.bar.baz'
        r = db.__send__(:create_attributes, now, row)
        expect(r[:type]).to eq 'foo'
      end
      context 'created_at is nil' do
        it 'status is :finished' do
          data[:created_at] = nil
          r = db.__send__(:create_attributes, now, row)
          expect(r[:status]).to eq TaskStatus::FINISHED
        end
      end
    end
    context 'created_at > 0' do
      context 'timeout' do
        it 'status is :waiting' do
          data[:created_at] = 1
          data[:timeout] = 0
          r = db.__send__(:create_attributes, now, row)
          expect(r[:status]).to eq TaskStatus::WAITING
        end
      end
      it 'status is :running' do
        data[:created_at] = 1
        data[:timeout] = now+100
        r = db.__send__(:create_attributes, now, row)
        expect(r[:status]).to eq TaskStatus::RUNNING
      end
    end
  end
end

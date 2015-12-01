require 'spec_helper'

describe PerfectQueue::Task do
  describe '.new' do
    it 'returns a Task' do
      task = Task.new(double, double)
      expect(task).to be_an_instance_of(Task)
    end
  end

  describe '#config' do
    it 'returns the client' do
      config = double('config')
      client = double('client', config: config)
      key = double('key')
      task = Task.new(client, key)
      expect(task).to be_an_instance_of(Task)
      expect(task.client).to eq(client)
      expect(task.config).to eq(config)
    end
  end

  describe '#preempt' do
    it 'returns inspected string' do
      client = double('client')
      key = double('key')
      task = Task.new(client, key)
      options = double('options')
      ret = double('ret')
      expect(client).to receive(:preempt).with(key, options).exactly(:once).and_return(ret)
      expect(task.preempt(options)).to eq(ret)
    end
  end

  describe '#inspect' do
    it 'returns inspected string' do
      key = double('key')
      task = Task.new(double('client'), key)
      expect(task.inspect).to eq("#<PerfectQueue::Task @key=#{key.inspect}>")
    end
  end

  describe '#update_data!' do
    context 'PLT-4238' do
      let (:config){ {type: 'rdb_compat', url: 'mysql://root:@localhost/perfectqueue_test', table: 'test_queues', type: 'rdb_compat'} }
      let (:client){ Client.new(config) }
      before do
        client.backend.db.tap{|s| s.tables.each{|t| s.drop_table(t) } }
        client.init_database
        client.submit('key', 'test1', {'foo' => 1}, {compression: 'gzip'})
      end
      it 'keeps the data compressed' do
        tasks = client.acquire
        expect(tasks.size).to eq 1
        task = tasks.first
        expect(task.compression).to eq 'gzip'
        task.update_data!('hoge' => 2)
        task.release!

        tasks = client.acquire
        task = tasks.first
        expect(tasks.size).to eq 1
        expect(task.compression).to eq 'gzip'
        data = task.data
        expect(data['foo']).to eq 1
        expect(data['hoge']).to eq 2
      end
    end
  end
end

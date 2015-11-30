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
end

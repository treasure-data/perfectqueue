require 'spec_helper'

describe PerfectQueue::TaskMetadata do
  let (:attributes){ double('attributes', delete: nil) }
  describe '#task' do
    it 'returns a task' do
      client = double('client')
      key = double('key')
      tm = TaskMetadata.new(client, key, attributes)
      task = tm.task
      expect(task).to be_a(Task)
      expect(task.client).to eq(client)
      expect(task.key).to eq(key)
    end
  end

  describe '#inspect' do
    it 'returns inspected string' do
      client = double('client')
      key = double('key')
      tm = TaskMetadata.new(client, key, attributes)
      expect(tm.inspect).to eq("#<PerfectQueue::TaskMetadata @key=#{key.inspect} @attributes=#{attributes.inspect}>")
    end
  end

  describe 'running?' do
    it 'returns true on running' do
      tm = TaskMetadata.new(double, double, status: TaskStatus::RUNNING)
      expect(tm.running?).to be true
    end

    it 'returns false on finished' do
      tm = TaskMetadata.new(double, double, status: TaskStatus::FINISHED)
      expect(tm.running?).to be false
    end
  end

  describe 'message' do
    it 'returns given message' do
      message = double('message')
      tm = TaskMetadata.new(double, double, message: message)
      expect(tm.message).to eq(message)
    end
  end

  describe 'user' do
    it 'returns given user' do
      user = double('user')
      tm = TaskMetadata.new(double, double, user: user)
      expect(tm.user).to eq(user)
    end
  end

  describe 'created_at' do
    it 'returns a time of given created_at' do
      epoch = 42
      tm = TaskMetadata.new(double, double, created_at: epoch)
      expect(tm.created_at).to eq(Time.at(epoch))
    end
  end

  describe 'timeout' do
    it 'returns given timeout' do
      epoch = 72
      tm = TaskMetadata.new(double, double, timeout: epoch)
      expect(tm.timeout).to eq(epoch)
    end
  end
end

require 'spec_helper'

describe PerfectQueue::Runner do
  describe '#new' do
    it 'creates with task' do
      expect(PerfectQueue::Runner.new(double('task'))).to be_a(PerfectQueue::Runner)
    end
  end

  describe '#task' do
    let (:task) { double('task') }
    let (:runner) { PerfectQueue::Runner.new(task) }
    it 'returns given task' do
      expect(runner.task).to eq(task)
    end
  end

  describe '#queue' do
    let (:runner) { PerfectQueue::Runner.new(double('task', client: 1)) }
    it 'returns a queue' do
      queue = runner.queue
      expect(queue).to be_a(PerfectQueue::Queue)
    end
  end

  describe '#kill' do
    let (:runner) { PerfectQueue::Runner.new(double('task')) }
    it 'always returns nil' do
      expect(runner.kill(nil)).to be_nil
    end
  end
end

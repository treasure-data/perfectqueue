require 'spec_helper'

describe PerfectQueue::Application::UndefinedDecisionError do
  it { is_expected.to be_an_instance_of(PerfectQueue::Application::UndefinedDecisionError) }
  it { is_expected.to be_a(Exception) }
end

describe PerfectQueue::Application::Decider do
  describe '#new' do
    let (:decider) { PerfectQueue::Application::Decider.new(nil) }
    it do
      expect(decider).to be_an_instance_of(PerfectQueue::Application::Decider)
    end
  end

  describe '#queue' do
    let (:queue){ double('queue') }
    let (:decider) do
      base = double('base')
      allow(base).to receive(:queue).exactly(:once).and_return(queue)
      PerfectQueue::Application::Decider.new(base)
    end
    it 'calls @base.queue' do
      expect(decider.queue).to eq(queue)
    end
  end

  describe '#task' do
    let (:task){ double('task') }
    let (:decider) do
      base = double('base')
      allow(base).to receive(:task).exactly(:once).and_return(task)
      PerfectQueue::Application::Decider.new(base)
    end
    it 'calls @base.task' do
      expect(decider.task).to eq(task)
    end
  end

  describe '#decide!' do
    let (:decider) { PerfectQueue::Application::Decider.new(nil) }
    it 'calls the specified method' do
      allow(decider).to receive(:foo).exactly(:once).with(72).and_return(42)
      expect(decider.decide!(:foo, 72)).to eq(42)
    end
    it 'raises UndefinedDecisionError on unknown method' do
      expect{ decider.decide!(:foo, 72) }.to raise_error(PerfectQueue::Application::UndefinedDecisionError)
    end
  end
end

describe PerfectQueue::Application::DefaultDecider do
  subject { PerfectQueue::Application::DefaultDecider.new(nil) }
  it { is_expected.to be_a(PerfectQueue::Application::Decider) }
  it { is_expected.to be_an_instance_of(PerfectQueue::Application::DefaultDecider) }
end

require 'spec_helper'

describe PerfectQueue::Application::Base do
  describe '.decider=' do
    it 'defines .decider which returns the decider' do
      decider_klass = double('decider_klass')
      klass = PerfectQueue::Application::Base
      allow(klass).to receive(:decider).and_call_original
      allow(klass).to receive(:decider=).with(decider_klass).and_call_original
      expect(klass.decider = decider_klass).to eq(decider_klass)
      expect(klass.decider).to eq(decider_klass)
    end
  end

  describe '.decider' do
    it 'returns DefaultDecider' do
      expect(PerfectQueue::Application::Base.decider).to eq(PerfectQueue::Application::DefaultDecider)
    end
  end

  describe '#new' do
    let (:task){ double('task') }
    let (:base) { PerfectQueue::Application::Base.new(task) }
    it 'calls super and set decider'do
      expect(base).to be_an_instance_of(PerfectQueue::Application::Base)
      expect(base.instance_variable_get(:@task)).to eq(task)
      expect(base.instance_variable_get(:@decider)).to be_an_instance_of(Application::DefaultDecider)
    end
  end

  describe '#run' do
    let (:base) { PerfectQueue::Application::Base.new(double('task')) }
    it 'returns nil if before_perform returns false' do
      allow(base).to receive(:before_perform).and_return(false)
      expect(base.run).to be_nil
    end
    it 'returns nil' do
      expect(base).to receive(:before_perform).exactly(:once).and_call_original
      expect(base).to receive(:perform).exactly(:once).and_return(nil)
      expect(base).to receive(:after_perform).exactly(:once).and_call_original
      expect(base.run).to be_nil
    end
    it 'calls unexpected_error_raised on error' do
      allow(base).to receive(:before_perform).exactly(:once).and_call_original
      allow(base).to receive(:perform).exactly(:once) { raise }
      allow(base).to receive(:decide!).with(:unexpected_error_raised, error: kind_of(Exception)).exactly(:once)
      expect(base.run).to be_nil
    end
  end

  describe '#before_perform' do
    let (:base) { PerfectQueue::Application::Base.new(double('task')) }
    it 'returns true' do
      expect(base.before_perform).to be true
    end
  end

  describe '#after_perform' do
    let (:base) { PerfectQueue::Application::Base.new(double('task')) }
    it 'returns nil' do
      expect(base.after_perform).to be_nil
    end
  end

  describe '#decide!' do
    let (:base) do
      decider = double('decider')
      expect(decider).to receive(:decide!).with(:type, :option).exactly(:once)
      decider_klass = double('decider_klass')
      allow(decider_klass).to receive(:new).with(kind_of(PerfectQueue::Application::Base)).and_return(decider)
      klass = PerfectQueue::Application::Base
      allow(klass).to receive(:decider).and_call_original
      allow(klass).to receive(:decider=).with(decider_klass).and_call_original
      klass.decider = decider_klass
      klass.new(double('task'))
    end
    it 'calls decider.decide' do
      expect(base.decide!(:type, :option)).to be_nil
    end
  end
end

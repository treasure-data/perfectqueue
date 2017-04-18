require 'spec_helper'

describe PerfectQueue::Multiprocess::ThreadProcessor do
  describe '.new' do
    it 'returns a Multiprocess::ThreadProcessor' do
      runner = double('runner')
      processor_id = double('processor_id')
      config = {}
      processor = Multiprocess::ThreadProcessor.new(runner, processor_id, config)
      expect(processor).to be_an_instance_of(Multiprocess::ThreadProcessor)
      expect(processor.instance_variable_get(:@processor_id)).to eq(processor_id)
    end
  end

  describe '#force_stop' do
    let (:processor) do
      config = {logger: double('logger').as_null_object}
      Multiprocess::ThreadProcessor.new(double('runner'), double('processor_id'), config)
    end
    it 'force_stop' do
      processor.force_stop
      expect(processor.instance_variable_get(:@finish_flag).set?).to be true
    end
  end

  describe '#run_loop' do
    let (:runner) do
      r = double('runner')
      allow(r).to receive(:after_child_end)
      r
    end
    let (:processor) do
      config = {logger: double('logger').as_null_object}
      Multiprocess::ThreadProcessor.new(runner, double('processor_id'), config)
    end
    it 'rescues error' do
      pq = object_double('PerfectQueue').as_stubbed_const
      allow(pq).to receive(:open).and_raise(RuntimeError)
      expect(runner).to receive(:after_child_end)
      processor.__send__(:run_loop)
    end
  end

  describe '#process' do
    let (:runner) do
      r = double('runner')
      allow(r).to receive(:new).and_raise(RuntimeError)
      r
    end
    let (:processor) do
      config = {logger: double('logger').as_null_object}
      Multiprocess::ThreadProcessor.new(runner, double('processor_id'), config)
    end
    it 'rescues error' do
      expect{processor.__send__(:process, double('task', key: 1))}.to raise_error(RuntimeError)
    end
  end
end

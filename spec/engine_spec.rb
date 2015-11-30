require 'spec_helper'

describe PerfectQueue::Engine do
  let (:logger){ double('logger').as_null_object }
  let (:engine) do
    config = {logger: logger, processor_type: :thread}
    Engine.new(double, config)
  end

  describe '.new' do
    it 'returns an Engine with ForkProcessor for processor_type: nil' do
      config = {logger: double('logger'), processor_type: nil}
      engine = Engine.new(double, config)
      expect(engine).to be_an_instance_of(Engine)
      expect(engine.processors).to be_a(Array)
      expect(engine.processors.size).to eq(1)
      expect(engine.processors[0]).to be_an_instance_of(Multiprocess::ForkProcessor)
    end
    it 'returns an Engine with ForkProcessor for processor_type: :process' do
      config = {logger: double('logger'), processor_type: :process}
      engine = Engine.new(double, config)
      expect(engine).to be_an_instance_of(Engine)
      expect(engine.processors).to be_a(Array)
      expect(engine.processors.size).to eq(1)
      expect(engine.processors[0]).to be_an_instance_of(Multiprocess::ForkProcessor)
    end
    it 'returns an Engine with ThreadProcessor for processor_type: :thread' do
      config = {logger: double('logger'), processor_type: :thread}
      engine = Engine.new(double, config)
      expect(engine).to be_an_instance_of(Engine)
      expect(engine.processors).to be_a(Array)
      expect(engine.processors.size).to eq(1)
      expect(engine.processors[0]).to be_an_instance_of(Multiprocess::ThreadProcessor)
    end
    it 'returns an Engine with ForkProcessor for processor_type: :invalid' do
      config = {logger: double('logger'), processor_type: :invalid}
      expect{Engine.new(double, config)}.to raise_error(ConfigError)
    end
  end

  describe '#run' do
    before do
      processor_klass = (PerfectQueue::Multiprocess::ThreadProcessor)
      allow(processor_klass).to receive(:new) do
        processor = double('processor')
        expect(processor).to receive(:keepalive).exactly(:twice)
        expect(processor).to receive(:stop)
        expect(processor).to receive(:join)
        processor
      end
      expect(engine).to receive(:sleep).with(0...2)
    end
    it 'runs until stopped' do
      Thread.start{sleep 1; engine.stop(true) }
      engine.run
    end
  end

  describe '#restart' do
    context 'previous num_processors is small' do
      it 'increase the number of processors' do
        config = {logger: logger, processor_type: :thread}
        engine = Engine.new(double, config)
        expect(engine.processors.size).to eq(1)
        config[:processors] = 3
        expect(engine.restart(true, config)).to eq(engine)
        expect(engine.processors.size).to eq(3)
      end
    end
    context 'previous num_processors is large' do
      it 'decrease the number of processors' do
        config = {logger: logger, processor_type: :thread, processors: 2}
        engine = Engine.new(double, config)
        config[:processors] = 1
        expect(engine.restart(true, config)).to eq(engine)
        expect(engine.processors.size).to eq(1)
      end
    end
    context 'same number of processors' do
      it 'decrease the number of processors' do
        config = {logger: logger, processor_type: :thread}
        engine = Engine.new(double, config)
        expect(engine.restart(true, config)).to eq(engine)
        expect(engine.processors.size).to eq(1)
      end
    end
  end

  describe '#stop' do
    let (:immediate){ double('immediate') }
    before do
      engine.processors.each do |c|
        expect(c).to receive(:stop).with(immediate)
      end
    end
    it '@processors.each {|c| c.stop(immediate) }' do
      expect(engine.stop(immediate)).to eq(engine)
      expect(engine.instance_variable_get(:@finish_flag).set?).to be true
    end
  end

  describe '#join' do
    before do
      engine.processors.each do |c|
        expect(c).to receive(:join)
      end
    end
    it '@processors.each {|c| c.join }' do
      expect(engine.join).to eq(engine)
    end
  end

  describe '#shutdown' do
    it 'calls stop and join' do
      immediate = double('immediate')
      expect(engine).to receive(:stop).with(immediate)
      expect(engine).to receive(:join)
      engine.shutdown(immediate)
    end
  end

  describe '#replace' do
    context 'already replaced' do
      before do
        engine.instance_variable_set(:@replaced_pid, double)
      end
      it 'returns nil' do
        expect(engine).not_to receive(:stop)
        expect(engine.replace(double, double)).to be_nil
      end
    end
    context 'not replaced yet' do
      it 'calls spawn with [$0]+ARGV' do
        immediate = double('immediate')
        expect(engine).to receive(:stop).with(immediate)
        expect(Process).to receive(:spawn).with(*([$0]+ARGV))
        engine.replace(immediate)
      end
      it 'calls spawn with given command' do
        immediate = double('immediate')
        command = double('command')
        expect(engine).to receive(:stop).with(immediate)
        expect(Process).to receive(:spawn).with(command)
        engine.replace(immediate, command)
      end
    end
  end

  describe '#logrotated' do
    before do
      engine.processors.each do |c|
        expect(c).to receive(:logrotated)
      end
    end
    it '@processors.each {|c| c.logrotated }' do
      engine.logrotated
    end
  end
end

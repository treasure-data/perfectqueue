require 'spec_helper'

describe PerfectQueue::Multiprocess::ForkProcessor do
  describe '.new' do
    it 'returns a PerfectQueue::Multiprocess::ForkProcessor' do
      runner = double('runner')
      processor_id = double('processor_id')
      config = {}
      processor = Multiprocess::ForkProcessor.new(runner, processor_id, config)
      expect(processor).to be_an_instance_of(Multiprocess::ForkProcessor)
      expect(processor.instance_variable_get(:@processor_id)).to eq(processor_id)
    end
  end

  describe '#restart' do
    let (:config_keys){[
      :child_heartbeat_limit,
      :child_kill_interval,
      :child_graceful_kill_limit,
      :child_fork_frequency_limit,
      :child_heartbeat_kill_delay,
    ]}
    let (:config){ {logger: double('logger').as_null_object} }
    let (:processor) {
      runner = double('runner')
      processor_id = double('processor_id')
      Multiprocess::ForkProcessor.new(runner, processor_id, config)
    }
    it 'sets config' do
      config_keys.each do |key|
        config[key] = double(key)
      end
      processor.restart(true, config)
      config_keys.each do |key|
        expect(processor.instance_variable_get("@#{key}".to_sym)).to eq(config[key])
      end
      expect(processor.instance_variable_get(:@config)).to eq(config)
    end
    it 'calls ChildProcessMonitor#start_killing if it has ChildProcessMonitor' do
      immediate = double('immediate')
      cpm = double('ChildProcessMonitor')
      expect(cpm).to receive(:start_killing).with(immediate).exactly(:once)
      processor.instance_variable_set(:@cpm, cpm)
      processor.restart(immediate, config)
    end
  end

  describe '#stop' do
    let (:processor) {
      runner = double('runner')
      processor_id = double('processor_id')
      config = {logger: double('logger').as_null_object}
      Multiprocess::ForkProcessor.new(runner, processor_id, config)
    }
    it 'calls ChildProcessMonitor#start_killing if it has ChildProcessMonitor' do
      immediate = double('immediate')
      cpm = double('ChildProcessMonitor')
      expect(cpm).to receive(:start_killing).with(immediate).exactly(:once)
      processor.instance_variable_set(:@cpm, cpm)
      processor.stop(immediate)
      expect(processor.instance_variable_get(:@stop)).to be true
    end
  end

  describe '#keepalive' do
    let (:processor) do
      config = {logger: double('logger').as_null_object}
      Multiprocess::ForkProcessor.new(double('runner'), double('processor_id'), config)
    end
    it 'tries join on stopping without cpm' do
      processor.stop(true)
      processor.keepalive
    end
    it 'tries join on stopping with cpm' do
      processor.stop(true)
      cpm = double('ChildProcessMonitor', try_join: false)
      processor.instance_variable_set(:@cpm, cpm)
      processor.keepalive
    end
    it 'calls fork_child if it doesn\'t have ChildProcessMonitor' do
      expect(processor.keepalive).to be_nil
      expect(processor.instance_variable_get(:@cpm)).to be_an_instance_of(Multiprocess::ChildProcessMonitor)
    end
    it 'rascues fork_child\'s error if it doesn\'t have ChildProcessMonitor' do
      allow(processor).to receive(:fork_child).and_raise(RuntimeError)
      expect(processor.keepalive).to be_nil
    end
    it 'tries join if it has killed ChildProcessMonitor' do
      cpm = double('ChildProcessMonitor', killing_status: true, try_join: true, cleanup: nil)
      processor.instance_variable_set(:@cpm, cpm)
      expect(processor.keepalive).to be_nil
      expect(processor.instance_variable_get(:@cpm)).to be_an_instance_of(Multiprocess::ChildProcessMonitor)
    end
    it 'recues EOFError of ChildProcessMonitor#check_heartbeat' do
      cpm = double('ChildProcessMonitor', killing_status: false, try_join: true, cleanup: nil, pid: 42)
      allow(cpm).to receive(:check_heartbeat).and_raise(EOFError)
      immediate = double('immediate')
      expect(cpm).to receive(:start_killing).with(true, processor.instance_variable_get(:@child_heartbeat_kill_delay)).exactly(:once)
      processor.instance_variable_set(:@cpm, cpm)
      expect(processor.keepalive).to be_nil
      expect(processor.instance_variable_get(:@cpm)).to be_an_instance_of(Multiprocess::ChildProcessMonitor)
    end
    it 'recues an error of ChildProcessMonitor#check_heartbeat' do
      cpm = double('ChildProcessMonitor', killing_status: false, try_join: true, cleanup: nil, pid: 42)
      allow(cpm).to receive(:check_heartbeat).and_raise(RuntimeError)
      immediate = double('immediate')
      expect(cpm).to receive(:start_killing).with(true, processor.instance_variable_get(:@child_heartbeat_kill_delay)).exactly(:once)
      processor.instance_variable_set(:@cpm, cpm)
      expect(processor.keepalive).to be_nil
      expect(processor.instance_variable_get(:@cpm)).to be_an_instance_of(Multiprocess::ChildProcessMonitor)
    end
    it 'calls ChildProcessMonitor#start_killing if it is dead' do
      cpm = double('ChildProcessMonitor', killing_status: false, check_heartbeat: false, try_join: true, cleanup: nil, pid: 42)
      immediate = double('immediate')
      expect(cpm).to receive(:start_killing).with(true).exactly(:once)
      processor.instance_variable_set(:@cpm, cpm)
      expect(processor.keepalive).to be_nil
      expect(processor.instance_variable_get(:@cpm)).to be_an_instance_of(Multiprocess::ChildProcessMonitor)
    end
  end

  describe '#join' do
    let (:processor) {
      config = {logger: double('logger').as_null_object, child_kill_interval: 0.1}
      Multiprocess::ForkProcessor.new(double('runner'), double('processor_id'), config)
    }
    it 'calls ChildProcessMonitor#start_killing if it has ChildProcessMonitor' do
      immediate = double('immediate')
      cpm = double('ChildProcessMonitor', cleanup: nil)
      allow(cpm).to receive(:try_join).and_return(false, true)
      processor.instance_variable_set(:@cpm, cpm)
      processor.join
    end
  end

  describe '#logrotated' do
    let (:processor) {
      config = {logger: double('logger').as_null_object}
      Multiprocess::ForkProcessor.new(double('runner'), double('processor_id'), config)
    }
    it 'calls ChildProcessMonitor#start_killing if it has ChildProcessMonitor' do
      immediate = double('immediate')
      cpm = double('ChildProcessMonitor')
      allow(cpm).to receive(:send_signal).with(:CONT).exactly(:once)
      processor.instance_variable_set(:@cpm, cpm)
      processor.logrotated
    end
  end

  describe '#fork_child' do
    it 'calls ChildProcessMonitor#start_killing if it has ChildProcessMonitor' do
      config = {logger: double('logger').as_null_object}
      processor = Multiprocess::ForkProcessor.new(double('runner'), double('processor_id'), config)
      processor.instance_variable_set(:@last_fork_time, Float::MAX)
      expect(processor.__send__(:fork_child)).to be_nil
    end
    it 'runs child process' do
      runner = double('runner')
      processor_id = double('processor_id')
      expect(runner).to receive(:after_fork).exactly(:once)
      expect(runner).to receive(:after_child_end).exactly(:once)
      config = {logger: double('logger').as_null_object}
      processor = Multiprocess::ForkProcessor.new(runner, processor_id, config)
      expect(processor).to receive(:fork).and_yield
      e = Exception.new
      allow(processor).to receive(:exit!).and_raise(e)
      expect{processor.__send__(:fork_child)}.to raise_error(e)
    end
  end
end

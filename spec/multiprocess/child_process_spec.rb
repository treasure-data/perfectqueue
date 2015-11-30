require 'spec_helper'

describe PerfectQueue::Multiprocess::ChildProcess do
  let (:runner_insntace){  double('runner') }
  let (:runner) do
    runner = double('Runner')
    allow(runner).to receive(:new).and_return(runner_insntace)
    runner
  end
  let (:processor_id){ double('processor_id') }
  let (:logger){ double('logger').as_null_object }
  let (:config){ {logger: logger} }
  let (:wpipe){ double('wpipe', sync: true, :'sync=' => true) }
  let (:pr){ Multiprocess::ChildProcess.new(runner, processor_id, config, wpipe) }
  describe '.run' do
    let (:pr){ double('child_process') }
    before do
      expect(Multiprocess::ChildProcess).to receive(:new).exactly(:once) \
        .with(runner, processor_id, config, wpipe).and_return(pr)
      expect(pr).to receive(:run).exactly(:once)
    end
    it 'runs an instance' do
      Multiprocess::ChildProcess.run(runner, processor_id, config, wpipe)
    end
  end

  describe '.new' do
    it 'returns a Multiprocess::ChildProcess' do
      pr = Multiprocess::ChildProcess.new(runner, processor_id, config, wpipe)
      expect(pr).to be_an_instance_of(Multiprocess::ChildProcess)
      expect(pr.instance_variable_get(:@wpipe)).to eq(wpipe)
      expect(pr.instance_variable_get(:@sig)).to be_a(SignalThread)
    end
  end

  describe '#stop' do
    it 'call super' do
      pr.stop(true)
    end
  end

  describe '#keepalive' do
    it { pr.keepalive }
  end

  describe '#logrotated' do
    it do
      expect(logger).to receive(:reopen!).with(no_args).exactly(:once)
      pr.logrotated
    end
  end

  describe '#child_heartbeat' do
    let (:packet){ Multiprocess::ChildProcess::HEARTBEAT_PACKET }
    it 'write HEARTBEAT_PACKET' do
      expect(wpipe).to receive(:write).with(packet).exactly(:once)
      pr.child_heartbeat
    end
    it 'rescue an error' do
      expect(wpipe).to receive(:write).with(packet).exactly(:once) \
        .and_raise(RuntimeError)
      expect(pr).to receive(:force_stop).exactly(:once)
      pr.child_heartbeat
    end
  end

  describe '#force_stop' do
    it 'calls exit! 137' do
      expect(Process).to receive(:kill).with(:KILL, Process.pid)
      expect(pr).to receive(:exit!).with(137).exactly(:once)
      pr.force_stop
    end
  end

  describe '#process' do
    let (:task){ double('task', key: double) }
    before do
      expect(runner_insntace).to receive(:run)
    end
    context 'max_request_per_child is nil' do
      it 'runs' do
        pr.process(task)
      end
    end
    context 'max_request_per_child is set' do
      before do
        pr.instance_variable_set(:@max_request_per_child, 2)
      end
      it 'counts children if request_per_child is still small' do
        expect(pr).not_to receive(:stop)
        pr.instance_variable_set(:@request_per_child, 1)
        pr.process(task)
        expect(pr.instance_variable_get(:@request_per_child)).to eq(2)
      end
      it 'stops children if request_per_child exceeds the limit' do
        expect(pr).to receive(:stop).with(false).exactly(:once)
        pr.instance_variable_set(:@request_per_child, 2)
        pr.process(task)
        expect(pr.instance_variable_get(:@request_per_child)).to eq(3)
      end
    end
  end

  context 'signal handling' do
    before do
      allow(PerfectQueue).to receive(:open) do
        flag = pr.instance_variable_get(:@finish_flag)
        Thread.pass until flag.set?
      end
    end

    it 'calls stop(false) SIGTERM' do
      expect(pr).to receive(:stop).with(false).and_call_original
      Process.kill(:TERM, Process.pid)
      pr.run
    end

    it 'calls stop(false) SIGINT' do
      expect(pr).to receive(:stop).with(false).and_call_original
      Process.kill(:INT, Process.pid)
      pr.run
    end

    it 'calls stop(true) SIGQUIT' do
      expect(pr).to receive(:stop).with(true).and_call_original
      Process.kill(:QUIT, Process.pid)
      pr.run
    end

    it 'calls stop(false) SIGUSR1' do
      expect(pr).to receive(:stop).with(false).and_call_original
      Process.kill(:USR1, Process.pid)
      pr.run
    end

    it 'calls stop(true) SIGHUP' do
      expect(pr).to receive(:stop).with(true).and_call_original
      Process.kill(:HUP, Process.pid)
      pr.run
    end

    it 'calls stop(false) on SIGCONT' do
      expect(pr).to receive(:stop).with(false).and_call_original
      Process.kill(:CONT, Process.pid)
      pr.run
    end

    it 'calls stop(true) on SIGWINCH' do
      expect(pr).to receive(:stop).with(true).and_call_original
      Process.kill(:WINCH, Process.pid)
      pr.run
    end

    it 'calls logrotated on SIGUSR2' do
      expect(pr).to receive(:logrotated){ pr.stop(true) }
      Process.kill(:USR2, Process.pid)
      pr.run
    end
  end
end

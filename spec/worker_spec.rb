require 'spec_helper'

describe PerfectQueue::Worker do
  let (:worker){ Worker.new(double, {}) }

  describe '.run' do
    let (:runner){ double }
    let (:config){ double }
    let (:worker){ double }
    context 'with config' do
      it 'calls Worker.new.run' do
        expect(worker).to receive(:run).with(no_args).exactly(:once)
        allow(Worker).to receive(:new).with(runner, config).and_return(worker)
        Worker.run(runner, config)
      end
    end
    context 'with block' do
      it 'calls Worker.new.run' do
        expect(worker).to receive(:run).with(no_args).exactly(:once)
        allow(Worker).to receive(:new).with(runner, nil).and_return(worker)
        Worker.run(runner)
      end
    end
  end

  describe '.new' do
    context 'with config' do
      it 'returns a worker' do
        expect(Worker.new(double, {})).to be_an_instance_of(Worker)
      end
      it 'has @detach_wait which is 10.0' do
        worker = Worker.new(double, {})
        expect(worker.instance_variable_get(:@detach_wait)).to eq(10.0)
      end
      it 'has @detach_wait which is configured by config[:detach_wait]' do
        detach_wait = double
        worker = Worker.new(double, {detach_wait: detach_wait})
        expect(worker.instance_variable_get(:@detach_wait)).to eq(detach_wait)
      end
    end
    context 'with block' do
      it 'returns a worker' do
        expect(Worker.new(double){ {} }).to be_an_instance_of(Worker)
      end
    end
  end

  describe '#run' do
    before do
      allow(worker).to receive(:install_signal_handlers)
      allow(worker.instance_variable_get(:@sv)).to receive(:run){sleep 1}
    end
    context 'normal and detach' do
      it do
        pid = double
        waitpid2_ret = nil
        allow(worker).to receive(:fork).and_return(pid)
        allow(Process).to receive(:kill).with(:INT, pid) do
          waitpid2_ret = [pid, double]
        end
        allow(Process).to receive(:waitpid2).and_return(waitpid2_ret)
        Thread.new{sleep 0.5;worker.detach}
        worker.run
      end
    end
    context 'wrong pid' do
      it 'ignores error and finish' do
        wrong_pid = $$ # pid of myself is not suitable for waitpid2 and raise ECHILD
        allow(worker).to receive(:fork).and_return(wrong_pid)
        expect{ worker.run }.not_to raise_error
      end
    end
    context 'child process side' do
      it 'run supervisor and exit!' do
        e = StandardError.new
        allow(worker).to receive(:fork).and_yield
        allow(worker.instance_variable_get(:@sv)).to receive(:run)
        allow(worker).to receive(:exit!).exactly(:once){raise e}
        expect{ worker.run }.to raise_error(e)
      end
    end
  end

  describe '#stop' do
    let (:worker){ Worker.new(double, {}) }
    context 'immediate=true' do
      it 'send_signal(:QUIT)' do
        expect(worker).to receive(:send_signal).with(:QUIT)
        worker.stop(true)
      end
    end
    context 'immediate=false' do
      it 'send_signal(:TERM)' do
        expect(worker).to receive(:send_signal).with(:TERM)
        worker.stop(false)
      end
    end
  end

  describe '#restart' do
    let (:worker){ Worker.new(double, {}) }
    context 'immediate=true' do
      it 'send_signal(:HUP)' do
        expect(worker).to receive(:send_signal).with(:HUP)
        worker.restart(true)
      end
    end
    context 'immediate=false' do
      it 'send_signal(:USR1)' do
        expect(worker).to receive(:send_signal).with(:USR1)
        worker.restart(false)
      end
    end
  end

  describe '#logrotated' do
    it 'send_signal(:USR2)' do
      expect(worker).to receive(:send_signal).with(:USR2)
      worker.logrotated
    end
  end

  describe '#detach' do
    it 'send_signal(:INT) and so on' do
      expect(worker).to receive(:send_signal).with(:INT)
      expect(worker.instance_variable_get(:@finish_flag)).to receive(:set!).with(no_args)
      worker.detach
      expect(worker.instance_variable_get(:@detach)).to be true
    end
  end

  describe '#send_signal' do
    let (:sig){ double }
    let (:pid){ double }
    before do
      worker.instance_variable_set(:@pid, pid)
    end
    context 'normal' do
      it 'kill the process' do
        allow(Process).to receive(:kill).with(sig, pid)
        worker.__send__(:send_signal, sig)
      end
    end
    context 'ESRCH' do
      it 'ignores ESRCH' do
        allow(Process).to receive(:kill).with(sig, pid).and_raise(Errno::ESRCH)
        worker.__send__(:send_signal, sig)
      end
    end
    context 'EPERM' do
      it 'ignores EPERM' do
        allow(Process).to receive(:kill).with(sig, pid).and_raise(Errno::EPERM)
        worker.__send__(:send_signal, sig)
      end
    end
  end

  describe '#install_signal_handlers' do
    let (:signal_thread){ worker.__send__(:install_signal_handlers) }
    before do
      signal_thread
    end
    after do
      signal_thread.stop
      signal_thread.value
      trap :TERM, 'DEFAULT'
      trap :INT, 'DEFAULT'
      trap :QUIT, 'DEFAULT'
      trap :USR1, 'DEFAULT'
      trap :HUP, 'DEFAULT'
      trap :USR2, 'DEFAULT'
    end
    context 'TERM' do
      it 'call #stop(false)' do
        flag = false
        expect(worker).to receive(:stop).with(false){flag = true}
        Process.kill(:TERM, $$)
        10.times{sleep 0.1;break if flag}
      end
    end
    context 'INT' do
      it 'call #detach' do
        flag = false
        expect(worker).to receive(:detach).with(no_args){flag = true}
        Process.kill(:INT, $$)
        10.times{sleep 0.1;break if flag}
      end
    end
    context 'QUIT' do
      it 'call #stop(true)' do
        flag = false
        expect(worker).to receive(:stop).with(true){flag = true}
        Process.kill(:QUIT, $$)
        10.times{sleep 0.1;break if flag}
      end
    end
    context 'USR1' do
      it 'call #restart(false)' do
        flag = false
        expect(worker).to receive(:restart).with(false){flag = true}
        Process.kill(:USR1, $$)
        10.times{sleep 0.1;break if flag}
      end
    end
    context 'HUP' do
      it 'call #restart(true)' do
        flag = false
        expect(worker).to receive(:restart).with(true){flag = true}
        Process.kill(:HUP, $$)
        10.times{sleep 0.1;break if flag}
      end
    end
    context 'USR2' do
      it 'call #logrotated' do
        flag = false
        expect(worker).to receive(:logrotated).with(no_args){flag = true}
        Process.kill(:USR2, $$)
        10.times{sleep 0.1;break if flag}
      end
    end
  end
end

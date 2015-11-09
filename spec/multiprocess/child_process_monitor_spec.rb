require 'spec_helper'

describe PerfectQueue::Multiprocess::ChildProcessMonitor do
  let (:rpipe){ double('rpipe') }
  let (:last_heartbeat){ 42 }
  let (:last_kill_time){ 42 }
  let (:processor_id){ double('processor_id') }
  let (:log){ double('log').as_null_object }
  let (:cpm) {
    cpm = Multiprocess::ChildProcessMonitor.new(log, processor_id, rpipe)
    cpm.instance_variable_set(:@last_heartbeat, last_heartbeat)
    cpm
  }
  let (:now){ 72 }
  describe '.new' do
    it 'returns a PerfectQueue::Multiprocess::ChildProcessMonitor' do
      processor = Multiprocess::ChildProcessMonitor.new(log, processor_id, rpipe)
      expect(processor).to be_an_instance_of(Multiprocess::ChildProcessMonitor)
    end
  end

  describe '#check_heartbeat' do
    before do
      allow(object_double('Time').as_stubbed_const).to \
        receive_message_chain(:now, :to_i).and_return(now)
    end
    context 'rpipe returns value' do
      before do
        expect(rpipe).to receive(:read_nonblock)
      end
      it 'returns true' do
        limit = double('limit')
        expect(cpm.check_heartbeat(limit)).to be true
        expect(cpm.instance_variable_get(:@last_heartbeat)).to eq(now)
      end
    end
    context 'rpipe.read_nonblock raises EINTR' do
      before do
        expect(rpipe).to receive(:read_nonblock).and_raise(Errno::EINTR)
      end
      it 'returns false if last_heartbeat is too old on interupt' do
        expect(cpm.check_heartbeat(now-last_heartbeat-1)).to be false
        expect(cpm.instance_variable_get(:@last_heartbeat)).to eq(last_heartbeat)
      end
      it 'returns true if last_heartbeat is enough new on interupt' do
        expect(cpm.check_heartbeat(now-last_heartbeat)).to be true
        expect(cpm.instance_variable_get(:@last_heartbeat)).to eq(last_heartbeat)
      end
    end
  end

  describe '#start_killing' do
    before do
      allow(object_double('Time').as_stubbed_const).to \
        receive_message_chain(:now, :to_i).and_return(now)
    end
    context 'initial state' do
      it 'calls kill_children immediately if immediate: true' do
        expect(cpm).to receive(:kill_children).with(now, nil).exactly(:once)
        cpm.start_killing(true)
        expect(cpm.instance_variable_get(:@kill_immediate)).to eq(true)
        expect(cpm.instance_variable_get(:@last_kill_time)).to eq(now)
        expect(cpm.instance_variable_get(:@kill_start_time)).to eq(now)
      end
      it 'sets @last_kill_time if immediate: true, delay!=0' do
        delay = 3
        expect(cpm).not_to receive(:kill_children)
        cpm.start_killing(true, delay)
        expect(cpm.instance_variable_get(:@kill_immediate)).to eq(true)
        expect(cpm.instance_variable_get(:@last_kill_time)).to eq(now+delay)
        expect(cpm.instance_variable_get(:@kill_start_time)).to eq(now+delay)
      end
    end
    context 'already killed immediately' do
      before do
        cpm.instance_variable_set(:@kill_immediate, true)
        cpm.instance_variable_set(:@last_kill_time, now)
        cpm.instance_variable_set(:@kill_start_time, now)
      end
      it 'returns without do anything if immediate: true' do
        expect(cpm).not_to receive(:kill_children)
        cpm.start_killing(true)
      end
      it 'returns without do anything if immediate: false' do
        expect(cpm).not_to receive(:kill_children)
        cpm.start_killing(false)
      end
    end
    context 'already started killing' do
      before do
        cpm.instance_variable_set(:@kill_start_time, double)
      end
      it 'return with do nothing if immediate: false' do
        cpm.start_killing(false, double)
      end
    end
  end

  describe '#killing_status' do
    context '@kill_start_time: nil' do
      before { cpm.instance_variable_set(:@kill_start_time, nil) }
      it 'returns nil' do
        expect(cpm.killing_status).to be_nil
      end
    end
    context '@kill_start_time: <time>' do
      before { cpm.instance_variable_set(:@kill_start_time, double) }
      context '@kill_immediate: true' do
        before { cpm.instance_variable_set(:@kill_immediate, true) }
        it 'returns nil' do
          expect(cpm.killing_status).to be true
        end
      end
      context '@kill_immediate: false' do
        before { cpm.instance_variable_set(:@kill_immediate, false) }
        it 'returns nil' do
          expect(cpm.killing_status).to be false
        end
      end
    end
  end

  describe '#try_join' do
    context 'not killed yet' do
      it 'returns nil' do
        expect(cpm).not_to receive(:kill_children)
        expect(cpm.try_join(double, double)).to be_nil
      end
    end
    context 'killing' do
      let (:cProcess) do
        allow(Process).to receive(:waitpid).with(processor_id, Process::WNOHANG)
      end
      before do
        cpm.instance_variable_set(:@kill_start_time, double)
      end
      context 'waitpid returns pid' do
        before do
          cProcess.and_return(processor_id)
          expect(cpm).not_to receive(:kill_children)
        end
        it 'returns true' do
          expect(cpm.try_join(double, double)).to be true
        end
      end
      context 'waitpid raises ECHILD' do
        before do
          cProcess.and_raise(Errno::ECHILD)
          expect(cpm).not_to receive(:kill_children)
        end
        it 'returns true' do
          expect(cpm.try_join(double, double)).to be true
        end
      end
      context 'waitpid returns nil' do
        before do
          cProcess.and_return(nil)
          allow(object_double('Time').as_stubbed_const).to \
            receive_message_chain(:now, :to_i).and_return(now)
          cpm.instance_variable_set(:@last_kill_time, last_kill_time)
        end
        it 'returns true if last_kill_time is new' do
          graceful_kill_limit = double('graceful_kill_limit')
          expect(cpm).to receive(:kill_children).with(now, graceful_kill_limit).exactly(:once)
          expect(cpm.try_join(30, graceful_kill_limit)).to be false
          expect(cpm.instance_variable_get(:@last_kill_time)).to eq(now)
        end
        it 'returns false if last_kill_time is old' do
          expect(cpm).not_to receive(:kill_children)
          expect(cpm.try_join(31, double)).to be false
        end
      end
    end
  end

  describe '#cleanup' do
    context 'rpipe is open' do
      it 'closes rpipe' do
        allow(rpipe).to receive(:closed?).and_return(false)
        expect(rpipe).to receive(:close).exactly(:once)
        cpm.cleanup
      end
    end
    context 'rpipe is closed' do
      it 'doesn\'t close rpipe' do
        allow(rpipe).to receive(:closed?).and_return(true)
        expect(rpipe).not_to receive(:close)
        cpm.cleanup
      end
    end
  end

  describe '#send_signal' do
    let (:sig){ double('sig') }
    let (:cProcess) do
      allow(Process).to receive(:kill).with(sig, processor_id)
    end
    context 'kill returnes pid' do
      before do
        cProcess.and_return(processor_id)
      end
      it { cpm.send_signal(sig) }
    end
    context 'kill raises ESRCH' do
      before{ cProcess.and_raise(Errno::ESRCH) }
      it { cpm.send_signal(sig) }
    end
    context 'kill raises EPERM' do
      before{ cProcess.and_raise(Errno::EPERM) }
      it { cpm.send_signal(sig) }
    end
  end

  describe '#kill_children' do
    context '@kill_start_time: nil' do
      # don't happen
    end
    context '@kill_start_time: <time>' do
      before do
        cpm.instance_variable_set(:@kill_start_time, 42)
      end
      context '@kill_immediate: true' do
        before do
          cpm.instance_variable_set(:@kill_immediate, true)
          expect(cpm).to receive(:get_ppid_pids_map).with(no_args).and_return({1=>processor_id}).exactly(:once)
          expect(cpm).to receive(:collect_child_pids).with({1=>processor_id}, [processor_id], processor_id) \
            .and_return([processor_id]).exactly(:once)
          expect(cpm).to receive(:kill_process).with(processor_id, true)
        end
        it 'calls kill_process immediately' do
          cpm.__send__(:kill_children, now, double)
        end
      end
      context '@kill_immediate: false' do
        before do
          cpm.instance_variable_set(:@kill_immediate, false)
        end
        it 'calls kill_process immediately' do
          expect(cpm).to receive(:get_ppid_pids_map).with(no_args).and_return({1=>processor_id}).exactly(:once)
          expect(cpm).to receive(:collect_child_pids).with({1=>processor_id}, [processor_id], processor_id) \
            .and_return([processor_id]).exactly(:once)
          expect(cpm).to receive(:kill_process).with(processor_id, true)
          cpm.__send__(:kill_children, now, 29)
        end
        it 'calls kill_process' do
          expect(cpm).not_to receive(:get_ppid_pids_map)
          expect(cpm).not_to receive(:collect_child_pids)
          expect(cpm).to receive(:kill_process).with(processor_id, false)
          cpm.__send__(:kill_children, now, 30)
        end
      end
    end
  end

  describe '#get_ppid_pids_map' do
    before do
      expect(cpm).to receive(:`).with('ps axo pid,ppid') \
        .and_return <<eom
  PID  PPID
      1     0
      2     1
      3     1
      4     2
      5     3
eom
    end
    it 'returns a tree of hash' do
      expect(cpm.__send__(:get_ppid_pids_map)).to eq({0=>[1], 1=>[2, 3], 2=>[4], 3=>[5]})
    end
  end

  describe '#collect_child_pids' do
    it 'returns a flat array of given children' do
      ppid_pids = {0=>[1], 1=>[2, 3], 2=>[4], 3=>[5]}
      parent_pid = 1
      results = cpm.__send__(:collect_child_pids, ppid_pids, [parent_pid], parent_pid)
      expect(results).to eq([1, 2, 4, 3, 5])
    end
  end

  describe '#kill_process' do
    let (:pid){ double('pid') }
    it  'kill(:KILL, pid) for immediate:true' do
      expect(Process).to receive(:kill).with(:KILL, pid).and_return(pid).exactly(:once)
      expect(cpm.__send__(:kill_process, pid, true)).to eq(pid)
    end
    it  'kill(:TERM, pid) for immediate:false' do
      expect(Process).to receive(:kill).with(:TERM, pid).and_return(pid).exactly(:once)
      expect(cpm.__send__(:kill_process, pid, false)).to eq(pid)
    end
    it 'rescues ESRCH' do
      expect(Process).to receive(:kill).with(:KILL, pid).and_raise(Errno::ESRCH).exactly(:once)
      expect(cpm.__send__(:kill_process, pid, true)).to be_nil
    end
    it 'rescues EPERM' do
      expect(Process).to receive(:kill).with(:KILL, pid).and_raise(Errno::EPERM).exactly(:once)
      expect(cpm.__send__(:kill_process, pid, true)).to be_nil
    end
  end
end

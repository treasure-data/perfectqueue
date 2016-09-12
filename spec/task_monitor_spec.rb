require 'spec_helper'

describe PerfectQueue::TaskMonitor do
  describe '#kill_task' do
    it 'rescues exception' do
      tm = PerfectQueue::TaskMonitor.new(logger: double('logger').as_null_object)
      task = double('task')
      reason = double('reason')
      allow(task).to receive_message_chain(:runner, :kill) \
        .with(no_args).with(reason){raise}
      tm.instance_variable_set(:@task, task)
      expect{tm.kill_task(reason)}.to raise_error(RuntimeError)
    end
  end

  describe '#external_task_heartbeat' do
    it 'rescues exception' do
      tm = PerfectQueue::TaskMonitor.new(logger: double('logger').as_null_object)
      task = double('task')
      reason = double('reason')
      epoch = double('epoch')
      allow(Time).to receive_message_chain(:now, :to_i){epoch}
      ret = double('ret')
      tm.instance_variable_set(:@task, task)
      expect(tm.external_task_heartbeat(task){ret}).to eq(ret)
      expect(tm.instance_variable_get(:@last_task_heartbeat)).to eq(epoch)
    end
  end

  describe '#run' do
    it 'rescues unknown error' do
      config = {logger: double('logger').as_null_object}
      force_stop = double('force_stop')
      expect(force_stop).to receive(:call).with(no_args).exactly(:once)
      tm = PerfectQueue::TaskMonitor.new(config, nil, force_stop)
      allow(Time).to receive(:now){raise}
      tm.run
    end
  end

  describe '#task_heartbeat' do
    let (:tm){ PerfectQueue::TaskMonitor.new(logger: double('logger').as_null_object) }
    let (:err){ StandardError.new('heartbeat preempted') }
    before do
      task = double('task')
      allow(task).to receive(:heartbeat!){ raise err }
      tm.set_task(task, double('runner'))
    end
    it 'calls kill_task($!) on heartbeat error' do
      expect(tm).to receive(:kill_task).with(err).exactly(:once)
      tm.__send__(:task_heartbeat)
    end
  end
end

describe PerfectQueue::TaskMonitorHook do
  let (:task) do
    obj = AcquiredTask.new(double(:client).as_null_object, 'key', {}, double)
    tm = TaskMonitor.new(logger: double('logger').as_null_object)
    tm.set_task(obj, double('runner'))
    obj
  end
  describe 'finish!' do
    it { task.finish! }
  end
  describe 'release!' do
    it { task.release! }
  end
  describe 'retry!' do
    it { task.retry! }
  end
  describe 'update_data!' do
    it { task.update_data!({}) }
  end
end

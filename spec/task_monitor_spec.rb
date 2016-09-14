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
    let (:config){ {type: 'rdb_compat', url: 'mysql2://root:@localhost/perfectqueue_test', table: 'test_queues', alive_time: 11} }
    let (:client){ Client.new(config) }
    let (:tm){ PerfectQueue::TaskMonitor.new(logger: double('logger').as_null_object, task_heartbeat_interval: 1) }
    let (:err){ StandardError.new('heartbeat preempted') }
    let (:now){ Time.now.to_i }
    let (:task){ double('task', timeout: now) }
    let (:runner){ double('runner') }
    context 'timeout' do
      before do
        tm.set_task(task, double('runner'))
      end
      it 'calls kill_task($!) on heartbeat error' do
        allow(task).to receive(:heartbeat!){ raise err }
        expect(tm).to receive(:kill_task).with(err).exactly(:once)
        tm.__send__(:task_heartbeat)
      end
    end
    context 'normal' do
      before do
        client.backend.db.tap{|s| s.tables.each{|t| s.drop_table(t) } }
        client.init_database
        client.submit('key', 'test1', {'foo' => 1}, {now: now-90,compression: 'gzip'})
        tm.start
      end
      after do
        tm.stop
        tm.join
      end
      it 'update timeout' do
        tasks = client.acquire(now: now-80)
        task = tasks[0]
        expect(task.timeout).to eq(now-80+config[:alive_time])
        allow(Time).to receive(:now).and_return(now-50)
        tm.set_task(task, runner, now-80)
        expect(task.timeout).to eq(now-50+config[:alive_time])
      end
    end
    context 'stolen' do
      before do
        client.backend.db.tap{|s| s.tables.each{|t| s.drop_table(t) } }
        client.init_database
        client.submit('key', 'test1', {'foo' => 1}, {now: now-90,compression: 'gzip'})
        tm.start
      end
      after do
        tm.stop
        tm.join
      end
      it 'raise error' do
        now1 = now - 80
        tasks = client.acquire(now: now1)
        task1 = tasks[0]
        expect(task1.timeout).to eq(now1+config[:alive_time])

        now2 = now - 50
        tasks = client.acquire(now: now2)
        task2 = tasks[0]
        expect(task2.timeout).to eq(now2+config[:alive_time])

        now3 = now - 20
        allow(Time).to receive(:now).and_return(now3)
        expect(runner).to receive(:kill)
        tm.set_task(task1, runner, now1)
      end
    end
    context 'timeout but can acquire' do
      before do
        client.backend.db.tap{|s| s.tables.each{|t| s.drop_table(t) } }
        client.init_database
        client.submit('key', 'test1', {'foo' => 1}, {now: now-90,compression: 'gzip'})
        tm.start
      end
      after do
        tm.stop
        tm.join
      end
      it 'raise error' do
        tasks = client.acquire(now: now-80)
        task1 = tasks[0]
        expect(task1.timeout).to eq(now-80+config[:alive_time])

        allow(Time).to receive(:now).and_return(now-50)
        tm.set_task(task1, runner, now-50)

        expect(task1.runner).to eq(runner)
      end
    end
  end
end

describe PerfectQueue::TaskMonitorHook do
  let (:task) do
    obj = AcquiredTask.new(double(:client).as_null_object, 'key', {timeout: Time.now.to_i}, double)
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

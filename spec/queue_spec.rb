require 'spec_helper'

describe Queue do
  include QueueTest

  it 'is a Queue' do
    expect(queue.class).to eq(PerfectQueue::Queue)
  end

  it 'succeess submit' do
    queue.submit('task01', 'type1', {})
  end

  it 'fail duplicated submit' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {}, :now=>now)

    expect {
      allow(STDERR).to receive(:puts)
      queue.submit('task01', 'type1', {}, :now=>now+1)
    }.to raise_error AlreadyExistsError

    queue['task01'].cancel_request!(:now=>now+2)

    expect {
      allow(STDERR).to receive(:puts)
      queue.submit('task01', 'type1', {}, :now=>now+10)
    }.to raise_error AlreadyExistsError
  end

  it 'list' do
    queue.submit('task01', 'type1', {"a"=>1})
    queue.submit('task02', 'type1', {"a"=>2})
    queue.submit('task03', 'type1', {"a"=>3})

    a = []
    queue.each {|t| a << t }
    a.sort_by! {|t| t.key }

    task01 = a.shift
    expect(task01.finished?).to eq(false)
    task01.type == 'type1'
    expect(task01.key).to eq('task01')
    expect(task01.data["a"]).to eq(1)

    task02 = a.shift
    expect(task02.finished?).to eq(false)
    task02.type == 'type1'
    expect(task02.key).to eq('task02')
    expect(task02.data["a"]).to eq(2)

    task03 = a.shift
    expect(task03.finished?).to eq(false)
    task03.type == 'type1'
    expect(task03.key).to eq('task03')
    expect(task03.data["a"]).to eq(3)

    expect(a.empty?).to eq(true)
  end

  it 'poll' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)
    queue.submit('task02', 'type1', {"a"=>2}, :now=>now+1)
    queue.submit('task03', 'type1', {"a"=>3}, :now=>now+2)

    task01 = queue.poll(:now=>now+10)
    expect(task01.key).to eq('task01')

    task02 = queue.poll(:now=>now+10)
    expect(task02.key).to eq('task02')

    task03 = queue.poll(:now=>now+10)
    expect(task03.key).to eq('task03')

    t4 = queue.poll(:now=>now+10)
    expect(t4).to eq(nil)
  end

  it 'release' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10)
    expect(task01.key).to eq('task01')

    task02 = queue.poll(:now=>now+10)
    expect(task02).to eq(nil)

    task01.release!(:now=>now+10)

    task03 = queue.poll(:now=>now+11)
    expect(task03.key).to eq('task01')
  end

  it 'timeout' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    expect(task01.key).to eq('task01')

    task02 = queue.poll(:now=>now+15)
    expect(task02).to eq(nil)

    task03 = queue.poll(:now=>now+20)
    expect(task03.key).to eq('task01')
  end

  it 'heartbeat' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    expect(task01.key).to eq('task01')

    task01.heartbeat!(:alive_time=>15, :now=>now+10)

    task02 = queue.poll(:now=>now+20)
    expect(task02).to eq(nil)

    task03 = queue.poll(:now=>now+30)
    expect(task03.key).to eq('task01')
  end

  it 'retry' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    expect(task01.key).to eq('task01')

    task01.retry!(:retry_wait=>15, :now=>now+10)

    task02 = queue.poll(:now=>now+20)
    expect(task02).to eq(nil)

    task03 = queue.poll(:now=>now+30)
    expect(task03.key).to eq('task01')
  end

  it 'froce_finish' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10)
    expect(task01.key).to eq('task01')

    expect(queue['task01'].metadata.running?).to eq(true)

    queue['task01'].force_finish!(:now=>now+11)

    expect(queue['task01'].metadata.finished?).to eq(true)
  end

  it 'status' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    # rdb_backend backend can't distinguish running with waiting
    #queue['task01'].metadata.finished?.should == false
    #queue['task01'].metadata.running?.should == false
    #queue['task01'].metadata.waiting?.should == true
    #queue['task01'].metadata.cancel_requested?.should == false

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    expect(task01.key).to eq('task01')

    expect(queue['task01'].metadata.finished?).to eq(false)
    expect(queue['task01'].metadata.running?).to eq(true)
    expect(queue['task01'].metadata.waiting?).to eq(false)
    expect(queue['task01'].metadata.cancel_requested?).to eq(false)

    task01.cancel_request!

    # status of cancel_requested running tasks is cancel_requested
    expect(queue['task01'].metadata.finished?).to eq(false)
    expect(queue['task01'].metadata.running?).to eq(false)
    expect(queue['task01'].metadata.waiting?).to eq(false)
    expect(queue['task01'].metadata.cancel_requested?).to eq(true)

    task01.finish!

    expect(queue['task01'].metadata.finished?).to eq(true)
    expect(queue['task01'].metadata.running?).to eq(false)
    expect(queue['task01'].metadata.waiting?).to eq(false)
    expect(queue['task01'].metadata.cancel_requested?).to eq(false)
  end

  it 'fail canceling finished task' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    expect(task01.key).to eq('task01')

    task01.finish!

    expect {
      allow(STDERR).to receive(:puts)
      queue['task01'].cancel_request!
    }.to raise_error AlreadyFinishedError
  end

  it 'retention_time' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    expect(queue['task01'].metadata.finished?).to eq(false)

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    expect(task01.key).to eq('task01')

    task01.finish!(:now=>now+11, :retention_time=>10)

    queue.poll(:now=>now+12)

    expect(queue['task01'].exists?).to eq(true)

    queue.poll(:now=>now+22)

    allow(STDERR).to receive(:puts)
    expect(queue['task01'].exists?).to eq(false)
  end

  it 'get_task_metadata failed with NotFoundError' do
    expect {
      allow(STDERR).to receive(:puts)
      queue['task99'].metadata
    }.to raise_error NotFoundError
  end

  it 'prefetch' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)
    queue.submit('task02', 'type2', {"a"=>2}, :now=>now+1)
    queue.submit('task03', 'type3', {"a"=>3}, :now=>now+2)

    tasks = queue.poll_multi(:now=>now+10, :alive_time=>10, :max_acquire=>2)
    expect(tasks.size).to eq(2)
    expect(tasks[0].key).to eq('task01')
    expect(tasks[1].key).to eq('task02')

    tasks = queue.poll_multi(:now=>now+10, :alive_time=>10, :max_acquire=>2)
    expect(tasks.size).to eq(1)
    expect(tasks[0].key).to eq('task03')

    tasks = queue.poll_multi(:now=>now+10, :alive_time=>10, :max_acquire=>2)
    expect(tasks).to eq(nil)
  end

  it 'data' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now)

    task01 = queue.poll(:now=>now+10)
    expect(task01.key).to eq('task01')
    expect(task01.data).to eq({"a"=>1})

    task01.update_data!({"b"=>2})
    expect(task01.data).to eq({"a"=>1, "b"=>2})

    task01.update_data!({"a"=>3,"c"=>4})
    expect(task01.data).to eq({"a"=>3, "b"=>2, "c"=>4})

    task01.release!

    task01 = queue.poll(:now=>now+10)
    expect(task01.key).to eq('task01')
    expect(task01.data).to eq({"a"=>3, "b"=>2, "c"=>4})
  end
end


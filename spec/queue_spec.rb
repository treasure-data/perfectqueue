require 'spec_helper'

describe Queue do
  include QueueTest

  it 'is a Queue' do
    queue.class.should == PerfectQueue::Queue
  end

  it 'succeess submit' do
    queue.submit('task01', 'type1', {})
  end

  it 'fail duplicated submit' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {}, :now=>now)

    lambda {
      queue.submit('task01', 'type1', {}, :now=>now+1)
    }.should raise_error AlreadyExistsError

    queue['task01'].cancel_request!(:now=>now+2)

    lambda {
      queue.submit('task01', 'type1', {}, :now=>now+10)
    }.should raise_error AlreadyExistsError
  end

  it 'list' do
    queue.submit('task01', 'type1', {"a"=>1})
    queue.submit('task02', 'type1', {"a"=>2})
    queue.submit('task03', 'type1', {"a"=>3})

    a = []
    queue.each {|t| a << t }
    a.sort_by! {|t| t.key }

    task01 = a.shift
    task01.finished?.should == false
    task01.type == 'type1'
    task01.key.should == 'task01'
    task01.retry_count.should == 0
    task01.data["a"].should == 1

    task02 = a.shift
    task02.finished?.should == false
    task02.type == 'type1'
    task02.key.should == 'task02'
    task01.retry_count.should == 0
    task02.data["a"].should == 2

    task03 = a.shift
    task03.finished?.should == false
    task03.type == 'type1'
    task03.key.should == 'task03'
    task01.retry_count.should == 0
    task03.data["a"].should == 3

    a.empty?.should == true
  end

  it 'poll' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)
    queue.submit('task02', 'type1', {"a"=>2}, :now=>now+1)
    queue.submit('task03', 'type1', {"a"=>3}, :now=>now+2)

    task01 = queue.poll(:now=>now+10)
    task01.key.should == 'task01'

    task02 = queue.poll(:now=>now+10)
    task02.key.should == 'task02'

    task03 = queue.poll(:now=>now+10)
    task03.key.should == 'task03'

    t4 = queue.poll(:now=>now+10)
    t4.should == nil
  end

  it 'release' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10)
    task01.key.should == 'task01'
    task01.retry_count.should == 0

    task02 = queue.poll(:now=>now+10)
    task02.should == nil

    task01.release!(:now=>now+10)

    task03 = queue.poll(:now=>now+11)
    task03.key.should == 'task01'
    task03.retry_count.should == 1
  end

  it 'timeout' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'
    task01.retry_count.should == 0

    task02 = queue.poll(:now=>now+15)
    task02.should == nil

    task03 = queue.poll(:now=>now+20)
    task03.key.should == 'task01'
    task03.retry_count.should == 1
  end

  it 'heartbeat' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'
    task01.retry_count.should == 0

    task01.heartbeat!(:alive_time=>15, :now=>now+10)

    task02 = queue.poll(:now=>now+20)
    task02.should == nil

    task03 = queue.poll(:now=>now+30)
    task03.key.should == 'task01'
    task03.retry_count.should == 1
  end

  it 'retry' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'
    task01.retry_count.should == 0

    task01.retry!(:retry_wait=>15, :now=>now+10)

    task02 = queue.poll(:now=>now+20)
    task02.should == nil

    task03 = queue.poll(:now=>now+30)
    task03.key.should == 'task01'
    task03.retry_count.should == 1
  end

  it 'froce_finish' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10)
    task01.key.should == 'task01'

    queue['task01'].metadata.running?.should == true

    queue['task01'].force_finish!(:now=>now+11)

    queue['task01'].metadata.finished?.should == true
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
    task01.key.should == 'task01'

    queue['task01'].metadata.finished?.should == false
    queue['task01'].metadata.running?.should == true
    queue['task01'].metadata.waiting?.should == false
    queue['task01'].metadata.cancel_requested?.should == false

    task01.cancel_request!

    # status of cancel_requested running tasks is cancel_requested
    queue['task01'].metadata.finished?.should == false
    queue['task01'].metadata.running?.should == false
    queue['task01'].metadata.waiting?.should == false
    queue['task01'].metadata.cancel_requested?.should == true

    task01.finish!

    queue['task01'].metadata.finished?.should == true
    queue['task01'].metadata.running?.should == false
    queue['task01'].metadata.waiting?.should == false
    queue['task01'].metadata.cancel_requested?.should == false
  end

  it 'fail canceling finished task' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'

    task01.finish!

    lambda {
      queue['task01'].cancel_request!
    }.should raise_error AlreadyFinishedError
  end

  it 'retention_time' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    queue['task01'].metadata.finished?.should == false

    task01 = queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'

    task01.finish!(:now=>now+11, :retention_time=>10)

    queue.poll(:now=>now+12)

    queue['task01'].exists?.should == true

    queue.poll(:now=>now+22)

    queue['task01'].exists?.should == false
  end

  it 'get_task_metadata failed with NotFoundError' do
    lambda {
      queue['task99'].metadata
    }.should raise_error NotFoundError
  end

  it 'prefetch' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)
    queue.submit('task02', 'type2', {"a"=>2}, :now=>now+1)
    queue.submit('task03', 'type3', {"a"=>3}, :now=>now+2)

    tasks = queue.poll_multi(:now=>now+10, :alive_time=>10, :max_acquire=>2)
    tasks.size.should == 2
    tasks[0].key.should == 'task01'
    tasks[1].key.should == 'task02'

    tasks = queue.poll_multi(:now=>now+10, :alive_time=>10, :max_acquire=>2)
    tasks.size.should == 1
    tasks[0].key.should == 'task03'

    tasks = queue.poll_multi(:now=>now+10, :alive_time=>10, :max_acquire=>2)
    tasks.should == nil
  end

  it 'data' do
    now = Time.now.to_i
    queue.submit('task01', 'type1', {"a"=>1}, :now=>now)

    task01 = queue.poll(:now=>now+10)
    task01.key.should == 'task01'
    task01.data.should == {"a"=>1}

    task01.update_data!({"b"=>2})
    task01.data.should == {"a"=>1, "b"=>2}

    task01.update_data!({"a"=>3,"c"=>4})
    task01.data.should == {"a"=>3, "b"=>2, "c"=>4}

    task01.release!

    task01 = queue.poll(:now=>now+10)
    task01.key.should == 'task01'
    task01.data.should == {"a"=>3, "b"=>2, "c"=>4}
  end
end


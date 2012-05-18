require 'spec_helper'

describe Queue do
  before do
    @queue = create_test_queue
  end

  after do
    @queue.client.close
  end

  it 'is a Queue' do
    @queue.class.should == PerfectQueue::Queue
  end

  it 'succeess submit' do
    @queue.submit('task01', 'type1', {})
  end

  it 'fail duplicated submit' do
    now = Time.now.to_i
    @queue.submit('task01', 'type1', {}, :now=>now)

    lambda {
      @queue.submit('task01', 'type1', {}, :now=>now+1)
    }.should raise_error AlreadyExistsError

    @queue['task01'].cancel_request!(:now=>now+2)

    lambda {
      @queue.submit('task01', 'type1', {}, :now=>now+10)
    }.should raise_error AlreadyExistsError
  end

  it 'list' do
    @queue.submit('task01', 'type1', {"a"=>1})
    @queue.submit('task02', 'type1', {"a"=>2})
    @queue.submit('task03', 'type1', {"a"=>3})

    a = []
    @queue.each {|t| a << t }
    a.sort_by! {|t| t.key }

    task01 = a.shift
    task01.finished?.should == false
    task01.type == 'type1'
    task01.key.should == 'task01'
    task01.data["a"].should == 1

    t2 = a.shift
    t2.finished?.should == false
    t2.type == 'type1'
    t2.key.should == 'task02'
    t2.data["a"].should == 2

    t3 = a.shift
    t3.finished?.should == false
    t3.type == 'type1'
    t3.key.should == 'task03'
    t3.data["a"].should == 3

    a.empty?.should == true
  end

  it 'poll' do
    now = Time.now.to_i
    @queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)
    @queue.submit('task02', 'type1', {"a"=>2}, :now=>now+1)
    @queue.submit('task03', 'type1', {"a"=>3}, :now=>now+2)

    task01 = @queue.poll(:now=>now+10)
    task01.key.should == 'task01'

    t2 = @queue.poll(:now=>now+10)
    t2.key.should == 'task02'

    t3 = @queue.poll(:now=>now+10)
    t3.key.should == 'task03'

    t4 = @queue.poll(:now=>now+10)
    t4.should == nil
  end

  it 'release' do
    now = Time.now.to_i
    @queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = @queue.poll(:now=>now+10)
    task01.key.should == 'task01'

    t2 = @queue.poll(:now=>now+10)
    t2.should == nil

    task01.release!(:now=>now+10)

    t3 = @queue.poll(:now=>now+11)
    t3.key.should == 'task01'
  end

  it 'timeout' do
    now = Time.now.to_i
    @queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = @queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'

    t2 = @queue.poll(:now=>now+15)
    t2.should == nil

    t3 = @queue.poll(:now=>now+20)
    t3.key.should == 'task01'
  end

  it 'heartbeat' do
    now = Time.now.to_i
    @queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = @queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'

    task01.heartbeat!(:alive_time=>15, :now=>now+10)

    t2 = @queue.poll(:now=>now+20)
    t2.should == nil

    t3 = @queue.poll(:now=>now+30)
    t3.key.should == 'task01'
  end

  it 'retry' do
    now = Time.now.to_i
    @queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = @queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'

    task01.retry!(:retry_wait=>15, :now=>now+10)

    t2 = @queue.poll(:now=>now+20)
    t2.should == nil

    t3 = @queue.poll(:now=>now+30)
    t3.key.should == 'task01'
  end

  it 'froce_finish' do
    now = Time.now.to_i
    @queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = @queue.poll(:now=>now+10)
    task01.key.should == 'task01'

    @queue['task01'].metadata.running?.should == true

    @queue['task01'].force_finish!(:now=>now+11)

    @queue['task01'].metadata.finished?.should == true
  end

  it 'status' do
    now = Time.now.to_i
    @queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    # rdb_backend backend can't distinguish running with waiting
    #@queue['task01'].metadata.finished?.should == false
    #@queue['task01'].metadata.running?.should == false
    #@queue['task01'].metadata.waiting?.should == true
    #@queue['task01'].metadata.cancel_requested?.should == false

    task01 = @queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'

    @queue['task01'].metadata.finished?.should == false
    @queue['task01'].metadata.running?.should == true
    @queue['task01'].metadata.waiting?.should == false
    @queue['task01'].metadata.cancel_requested?.should == false

    task01.cancel_request!

    # status of cancel_requested running tasks is cancel_requested
    @queue['task01'].metadata.finished?.should == false
    @queue['task01'].metadata.running?.should == false
    @queue['task01'].metadata.waiting?.should == false
    @queue['task01'].metadata.cancel_requested?.should == true

    task01.finish!

    @queue['task01'].metadata.finished?.should == true
    @queue['task01'].metadata.running?.should == false
    @queue['task01'].metadata.waiting?.should == false
    @queue['task01'].metadata.cancel_requested?.should == false
  end

  it 'fail canceling finished task' do
    now = Time.now.to_i
    @queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    task01 = @queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'

    task01.finish!

    lambda {
      @queue['task01'].cancel_request!
    }.should raise_error AlreadyFinishedError
  end

  it 'retention_time' do
    now = Time.now.to_i
    @queue.submit('task01', 'type1', {"a"=>1}, :now=>now+0)

    @queue['task01'].metadata.finished?.should == false

    task01 = @queue.poll(:now=>now+10, :alive_time=>10)
    task01.key.should == 'task01'

    task01.finish!(:now=>now+11, :retention_time=>10)

    @queue.poll(:now=>now+12)

    @queue['task01'].exists?.should == true

    @queue.poll(:now=>now+22)

    @queue['task01'].exists?.should == false
  end

  it 'get_task_metadata failed with NotFoundError' do
    lambda {
      @queue['task99'].metadata
    }.should raise_error NotFoundError
  end
end


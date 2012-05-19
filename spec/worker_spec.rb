require 'spec_helper'

class TestHandler < PerfectQueue::Application::Base
  def run
    puts "TestHandler: #{task}"
    if task.data['raise_error']
      raise "expected error test"
    end
    if num = task.data['sleep']
      sleep num
    end
    puts "Task finished"
  end

  def kill(reason)
    puts "kill: #{reason.class}: #{reason}"
  end
end

class RegexpHandler < PerfectQueue::Application::Base
  def run
    puts "RegexpHandler: #{task}"
  end
end

class TestApp < PerfectQueue::Application::Dispatch
  route 'test' => TestHandler
  route /reg.*/ => RegexpHandler
end

describe Worker do
  before do
    create_test_queue.close
    @worker = Worker.new(TestApp, test_queue_config)
    @thread = Thread.new {
      @worker.run
    }
  end

  after do
    @worker.stop(true)
    @thread.join
  end

  def submit(*args)
    queue = get_test_queue
    queue.submit(*args)
    queue.close
  end

  it 'route' do
    TestHandler.any_instance.should_receive(:run).once
    RegexpHandler.any_instance.should_receive(:run).once
    submit('task01', 'test', {})
    submit('task02', 'reg01', {})
    sleep 1
  end

  it 'term signal' do
    sleep 1
    Process.kill(:TERM, Process.pid)
    puts "finish expected..."
    @thread.join
  end

  it 'quit signal' do
    sleep 1
    Process.kill(:QUIT, Process.pid)
    puts "finish expected..."
    @thread.join
  end

  it 'kill reason' do
    TestHandler.any_instance.should_receive(:kill).once #.with(kind_of(PerfectQueue::CancelRequestedError))  # FIXME 'with' dead locks
    submit('task01', 'test', {'sleep'=>4})
    sleep 2
    Process.kill(:TERM, Process.pid)
    @thread.join
  end
end


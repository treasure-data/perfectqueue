require 'spec_helper'

class TestHandler < PerfectQueue::Application::Base
  #def before_run
  #  if task.data['no_run']
  #    false
  #  else
  #    true
  #  end
  #end

  def run
    puts "TestHandler: #{task}"
    if task.data['raise_error']
      raise "expected error test"
    end
    if num = task.data['sleep']
      sleep num
    end
    puts "task finished"
  end

  def kill(reason)
    puts "#{reason.class}: #{reason}"
  end
end

class RegexpHandler < PerfectQueue::Application::Base
  def run
    puts "RegexpHandler: #{task}"
  end
end

class TestApp < PerfectQueue::Application::Dispatch
  LATER = []

  def self.later(&block)
    LATER << block
  end

  def self.new(task)
    # TODO rspec doesn't work with fork?
    #LATER.each {|block| block.call }
    super
  end

  route 'test' => TestHandler
  route /reg.*/ => RegexpHandler
end

describe Worker do
  before do
    create_test_queue.close
    TestApp::LATER.clear
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
    TestApp.later do
      TestHandler.any_instance.should_receive(:run)
      RegexpHandler.any_instance.should_receive(:run)
    end
    submit('task01', 'test', {})
    submit('task02', 'reg01', {})
    sleep 1
  end

#  it 'before_run' do
#    TestApp.later do
#      TestHandler.any_instance.should_receive(:run).once
#    end
#    submit('task01', 'test', {})
#    submit('task02', 'test', {'no_run'=>true})
#    sleep 1
#  end

#  it 'after_run' do
#    TestApp.later do
#      TestHandler.any_instance.should_receive(:after_run).twice
#    end
#    submit('task01', 'test', {})
#    submit('task02', 'test', {'raise_error'=>true})
#    sleep 1
#  end

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
    TestApp.later do
      TestHandler.any_instance.should_receive(:kill)
    end
    submit('task01', 'test', {'sleep'=>4})
    sleep 2
    Process.kill(:TERM, Process.pid)
    @thread.join
  end
end


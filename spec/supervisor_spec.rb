require 'spec_helper'

class TestHandler < PerfectQueue::Application::Base
  def run
    #puts "TestHandler: #{task}"
    if task.data['raise_error']
      raise "expected error test"
    end
    if num = task.data['sleep']
      sleep num
    end
    #puts "Task finished"
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

describe Supervisor do
  include QueueTest

  before do
    logger = instance_double("DaemonsLogger")
    allow(logger).to receive(:info)
    allow(logger).to receive(:error)
    allow(logger).to receive(:warn)
    allow(logger).to receive(:close)
    allow(DaemonsLogger).to receive(:new){ logger }
    @sv = Supervisor.new(TestApp, queue_config)
    @thread = Thread.new {
      @sv.run
    }
  end

  after do
    @sv.stop(true)
    @thread.join
  end

  it 'route' do
    expect_any_instance_of(TestHandler).to receive(:run).once
    expect_any_instance_of(RegexpHandler).to receive(:run).once
    queue.submit('task01', 'test', {})
    queue.submit('task02', 'reg01', {})
    sleep 1
  end

  it 'term signal' do
    sleep 1
    Process.kill(:TERM, Process.pid)
    expect(@thread.join(3)).to eq(@thread)
  end

  it 'quit signal' do
    sleep 1
    Process.kill(:QUIT, Process.pid)
    #puts "finish expected..."
    expect(@thread.join(3)).to eq(@thread)
  end

  it 'kill reason' do
    expect_any_instance_of(TestHandler).to receive(:kill).once #.with(kind_of(PerfectQueue::CancelRequestedError))  # FIXME 'with' dead locks
    queue.submit('task01', 'test', {'sleep'=>4})
    sleep 2
    Process.kill(:TERM, Process.pid)
    expect(@thread.join(3)).to eq(@thread)
  end
end


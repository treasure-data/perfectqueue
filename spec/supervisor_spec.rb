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
  let (:logger) { double('logger').as_null_object }
  before do
    object_double('PerfectQueue::DaemonsLogger', new: logger).as_stubbed_const
  end

  context 'normal routing' do
    before do
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
      sleep 2
    end
  end

  context 'listen_debug_server' do
    after do
      @sv.stop(true)
      @thread.join
    end

    it 'listen_debug_server with UNIX Socket' do
      Tempfile.open('supervisor') do |f|
        config = queue_config.dup
        config[:debug] = f.path
        @sv = Supervisor.new(TestApp, config)
        @thread = Thread.new {
          @sv.run
        }
        sleep 2
      end
    end

    it 'listen_debug_server with TCP with address:port' do
      config = queue_config.dup
      config[:debug] = '127.0.0.1:0'
      @sv = Supervisor.new(TestApp, config)
      @thread = Thread.new {
        @sv.run
      }
      sleep 2
    end

    it 'listen_debug_server with TCP with port' do
      config = queue_config.dup
      config[:debug] = '0'
      @sv = Supervisor.new(TestApp, config)
      @thread = Thread.new {
        @sv.run
      }
      sleep 2
    end
  end

  context 'replace' do
    before do
      @sv = Supervisor.new(TestApp, queue_config)
      @thread = Thread.new {
        @sv.run
      }
      Thread.pass until @sv.engine
    end

    after do
      @sv.stop(true)
      @thread.join
    end

    it 'replaces immediately' do
      @sv.replace(true, ':')
    end

    it 'replaces not immediately' do
      @sv.replace(false, ':')
    end

    it 'fails to replace' do
      Thread.pass until @sv.engine
      allow(@sv.engine).to receive(:replace) { raise }
      @sv.replace(false, ':')
    end
  end

  context 'signal handling' do
    before do
      @sv = Supervisor.new(TestApp, queue_config)
      @thread = Thread.new {
        @sv.run
      }
    end

    after do
      @sv.stop(true)
      @thread.join
    end

    it 'handles TERM signal' do
      Thread.pass until @sv.engine
      Process.kill(:TERM, Process.pid)
      expect(@thread.join(3)).to eq(@thread)
    end

    it 'handles INT signal' do
      Thread.pass until @sv.engine
      Process.kill(:INT, Process.pid)
      expect(@thread.join(3)).to eq(@thread)
    end

    it 'handles QUIT signal' do
      Thread.pass until @sv.engine
      Process.kill(:QUIT, Process.pid)
      #puts "finish expected..."
      expect(@thread.join(3)).to eq(@thread)
    end

    it 'handles USR1 signal' do
      Thread.pass until @sv.engine
      processors = @sv.engine.processors
      Process.kill(:USR1, Process.pid)
      expect(@sv.engine.processors).to eq(processors)
    end

    it 'handles HUP signal' do
      Thread.pass until @sv.engine
      processors = @sv.engine.processors
      Process.kill(:HUP, Process.pid)
      expect(@sv.engine.processors).to eq(processors)
    end

    it 'handles USR2 signal' do
      Thread.pass until @sv.engine
      allow(logger).to receive(:reopen!)
      Process.kill(:USR2, Process.pid)
    end

    it 'kill reason' do
      expect_any_instance_of(TestHandler).to receive(:kill).once #.with(kind_of(PerfectQueue::CancelRequestedError))  # FIXME 'with' dead locks
      queue.submit('task01', 'test', {'sleep'=>4})
      sleep 2
      Process.kill(:TERM, Process.pid)
      expect(@thread.join(3)).to eq(@thread)
    end
  end

  describe '.run' do
    let (:runner) { double('runner') }
    let (:config) { double('config') }
    before (:each) do
      allow(Supervisor).to receive(:new) \
        .with(runner, config)  do |*args, &block|
        expect(block).to be_a(Proc)
        double('supervisor', run: nil)
      end
    end
    it 'calls Supervisor.new.run' do
      expect(Supervisor.run(runner, config){ }).to be_nil
    end
  end

  describe '#run' do
    let (:supervisor) { Supervisor.new(double('runner')){raise} }
    it 'rescues exception' do
      expect(supervisor.run).to be_nil
    end
  end

  describe '#stop' do
    let (:supervisor) { Supervisor.new(double('runner')){} }
    it 'return nil without engine' do
      expect(supervisor.run).to be_nil
    end
    it 'rescues exception' do
      supervisor.instance_variable_set(:@engine, true) # dummy
      expect(supervisor.stop(true)).to be false
    end
  end

  describe '#restart' do
    let (:supervisor) { Supervisor.new(double('runner')){} }
    it 'return nil without engine' do
      expect(supervisor.run).to be_nil
    end
    it 'rescues exception' do
      expect(supervisor.restart(true)).to be false
    end
  end
end

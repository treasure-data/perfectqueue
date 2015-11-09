require 'spec_helper'

describe SignalThread do
  before(:each) do
    @th = SignalThread.new
  end

  after(:each) do
    @th.trap('SIGUSR1')
    @th.stop
    @th.join
  end

  context('trap') do
    it 'ignores Signal' do
      old = @th.trap('SIGUSR1', 'IGNORE')
      expect(old).to be_nil
      expect(@th.handlers).to eq({})
    end

    it 'traps Signal' do
      flag = false
      pr = proc{flag=1;raise}
      expect(@th.trap('SIGUSR1', &pr)).to be_nil
      expect(@th.handlers).to eq({USR1: pr})
      allow(STDERR).to receive(:write).at_least(:once)
      Process.kill(:USR1, Process.pid)
      Thread.pass until flag
    expect(flag).to eq(1)
    end
  end

  it 'queues signal' do
    flag = false
    pr = proc{flag=1;raise}
    allow(@th).to receive(:enqueue){flag=2;raise}
    expect(@th.trap('SIGUSR1', &pr)).to be_nil
    allow(STDERR).to receive(:write).at_least(:once)
    Process.kill(:USR1, Process.pid)
    Thread.pass until flag
    expect(flag).to eq(2)
  end
end

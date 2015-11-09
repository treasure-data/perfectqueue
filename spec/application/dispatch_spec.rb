require 'spec_helper'

describe PerfectQueue::Application::Dispatch do
  describe '.new' do
    before do
      router = Application::Dispatch.router
      handler = double('handler')
      allow(handler).to receive(:new).and_return(nil)
      router.add(/\Afoo\z/, handler, nil)
    end
    it 'returns a PerfectQueue::Application::Dispatch' do
      task = double('task', type: 'foo')
      dispatch = Application::Dispatch.new(task)
      expect(dispatch).to be_an_instance_of(Application::Dispatch)
    end
    it 'raises RuntimeError if the task type doesn\'t match' do
      task = double('task', type: 'bar')
      expect(task).to receive(:retry!).exactly(:once)
      expect{Application::Dispatch.new(task)}.to raise_error(RuntimeError)
    end
  end
end

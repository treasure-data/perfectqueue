require 'spec_helper'

describe PerfectQueue::Client do
  describe '#preempt' do
    it '(key)' do
      backend = double('backend')
      alive_time = double('alive_time')
      object_double('PerfectQueue::Backend', new_backend: backend).as_stubbed_const
      client = Client.new({alive_time: alive_time})
      ret = double('ret')
      key = double('key')
      expect(backend).to receive(:preempt).with(key, alive_time, {}).and_return(ret)
      expect(client.preempt(key)).to eq(ret)
    end

    it '(key, options)' do
      backend = double('backend')
      alive_time = double('alive_time')
      object_double('PerfectQueue::Backend', new_backend: backend).as_stubbed_const
      client = Client.new({alive_time: alive_time})
      ret = double('ret')
      key = double('key')
      options = {alive_time: alive_time}
      expect(backend).to receive(:preempt).with(key, alive_time, options).and_return(ret)
      expect(client.preempt(key, options)).to eq(ret)
    end
  end
end

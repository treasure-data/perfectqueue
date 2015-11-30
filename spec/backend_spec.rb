require 'spec_helper'

describe PerfectQueue::Backend do
  describe '.new_backend' do
    it 'raises error if config[:type] is nil' do
      expect{Backend.new_backend(nil, {})}.to raise_error(ConfigError)
    end
  end
end

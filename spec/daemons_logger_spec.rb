require 'spec_helper'

describe DaemonsLogger do
  context 'new' do
    it 'creates logger with path string' do
      Tempfile.open('daemons_logger') do |io|
        logger = DaemonsLogger.new(io.path)
        expect(logger.class).to eq(DaemonsLogger)
        logger.close
        logger.close
      end
    end

    it 'creates logger with IO object' do
      io = double('dummy io', write: nil, close: nil)
      expect(DaemonsLogger.new(io).class).to eq(DaemonsLogger)
    end
  end

  context 'reopen' do
    it 'reopens IOs' do
      Tempfile.open('daemons_logger') do |f|
        logger = DaemonsLogger.new(f.path)
        expect(STDOUT).to receive(:reopen).twice
        logger.hook_stdout!
        expect(STDERR).to receive(:reopen).twice
        logger.hook_stderr!
        logger.reopen
        io = logger.instance_variable_get(:@log)
        allow(logger).to receive(:reopen!) { raise }
        logger.reopen
      end
    end
  end
end

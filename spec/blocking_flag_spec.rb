require 'spec_helper'

describe PerfectQueue::BlockingFlag do
  describe '.new' do
    it 'returns a BlockingFlag' do
      flag = BlockingFlag.new
      expect(flag).to be_an_instance_of(BlockingFlag)
    end
  end

  describe '#set!' do
    let (:flag){ BlockingFlag.new }
    it 'returns true if it was false' do
      expect(flag.set?).to eq false
      expect(flag.set!).to eq true
      expect(flag.set?).to eq true
    end
    it 'returns false if it was already true' do
      flag.set!
      expect(flag.set?).to eq true
      expect(flag.set!).to eq false
      expect(flag.set?).to eq true
    end
  end

  describe '#reset!' do
    let (:flag){ BlockingFlag.new }
    it 'returns false if it was already false' do
      expect(flag.set?).to eq false
      expect(flag.reset!).to eq false
      expect(flag.set?).to eq false
    end
    it 'returns false if it was true' do
      flag.set!
      expect(flag.set?).to eq true
      expect(flag.reset!).to eq true
      expect(flag.set?).to eq false
    end
  end

  describe '#set_region' do
    let (:flag){ BlockingFlag.new }
    it 'set in the block and reset it was set' do
      flag.set!
      flag.set_region do
        expect(flag.set?).to eq true
      end
      expect(flag.set?).to eq false
    end
    it 'set in the block and reset if it was reset' do
      flag.reset!
      flag.set_region do
        expect(flag.set?).to eq true
      end
      expect(flag.set?).to eq false
    end
    it 'set in the block and reset even if it raiess error' do
      flag.set_region do
        expect(flag.set?).to eq true
        raise
      end rescue nil
      expect(flag.set?).to eq false
    end
  end

  describe '#reset_region' do
    let (:flag){ BlockingFlag.new }
    it 'reset in the block and set it was set' do
      flag.set!
      flag.reset_region do
        expect(flag.set?).to eq false
      end
      expect(flag.set?).to eq true
    end
    it 'reset in the block and set if it was reset' do
      flag.reset!
      flag.reset_region do
        expect(flag.set?).to eq false
      end
      expect(flag.set?).to eq true
    end
    it 'set in the block and reset even if it raiess error' do
      flag.reset_region do
        expect(flag.set?).to eq false
        raise
      end rescue nil
      expect(flag.set?).to eq true
    end
  end

  describe '#wait' do
    let (:flag){ BlockingFlag.new }
    it 'wait until a thread set/reset the flag' do
      th1 = Thread.start do
        flag.wait(5)
        expect(flag.set?).to eq true
      end
      Thread.pass until th1.stop?
      flag.set!
      th1.join(2)
    end
  end
end

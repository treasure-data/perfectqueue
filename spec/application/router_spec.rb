require 'spec_helper'

describe PerfectQueue::Application::Router do
  describe '.new' do
    it 'returns a PerfectQueue::Application::Router' do
      router = Application::Router.new
      expect(router).to be_an_instance_of(Application::Router)
    end
  end

  describe '#add' do
    let (:router){ Application::Router.new }
    let (:sym){ double('sym') }
    it 'accepts Regexp' do
      router.add(/\Afoo\z/, sym, double)
      expect(router.patterns[0]).to eq([/\Afoo\z/, sym])
    end
    it 'accepts String' do
      router.add('foo', sym, double)
      expect(router.patterns[0]).to eq([/\Afoo\z/, sym])
    end
    it 'accepts Symbol' do
      router.add(:foo, sym, double)
      expect(router.patterns[0]).to eq([/\Afoo\z/, sym])
    end
    it 'raises for others' do
      expect{router.add(nil, nil, nil)}.to raise_error(ArgumentError)
    end
  end

  describe '#route' do
    let (:router) do
      rt = Application::Router.new
      rt.add(/\Afoo\z/, :TestHandler, double)
      rt
    end
    let (:handler){ double('handler') }
    before do
      Application::Router::TestHandler = handler
    end
    after do
      Application::Router.__send__(:remove_const, :TestHandler)
    end
    it 'return related handler' do
      expect(router.route('foo')).to eq(handler)
    end
  end
end

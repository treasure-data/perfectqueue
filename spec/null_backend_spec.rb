require 'spec_helper'
require 'perfectqueue/backend/null'

describe Backend::NullBackend do
  let (:backend){ Backend::NullBackend.new }
  describe '#list' do
    subject { backend.list{} }
     it { is_expected.to be_nil }
  end
  describe '#acquire' do
    subject { backend.acquire(double('timeout')) }
     it { is_expected.to be_nil }
  end
  describe '#finish' do
    subject { backend.finish(double('token')) }
     it { is_expected.to be true }
  end
  describe '#update' do
    subject { backend.update(double('token'), double('timeout')) }
     it { is_expected.to be_nil }
  end
  describe '#cancel' do
    subject { backend.cancel(double('id')) }
     it { is_expected.to be true }
  end
  describe '#submit' do
    subject { backend.submit(double('id'), double('data')) }
     it { is_expected.to be true }
  end
end

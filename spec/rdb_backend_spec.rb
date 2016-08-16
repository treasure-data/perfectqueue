require 'spec_helper'
require 'perfectqueue/backend/rdb'

describe Backend::RDBBackend do
  let (:now){ Time.now.to_i }
  let (:uri){ 'mysql2://root:@localhost/perfectqueue_test' }
  let (:table){ 'test_queues' }
  let (:db) do
    d = Backend::RDBCompatBackend.new(double, url: uri, table: table)
    s = d.db
    s.tables.each{|t| s.drop_table(t) }
    d.init_database({})
    Backend::RDBBackend.new(uri, table)
  end

  context '.new' do
    it 'supports mysql' do
      expect(Backend::RDBBackend.new(uri, table)).to be_an_instance_of(Backend::RDBBackend)
    end
  end

  context '#submit' do
    it 'adds task' do
      expect(db.submit('key', '{"foo":"bar"}')).to be true
      row = db.db.fetch("SELECT * FROM `#{table}` WHERE id=? LIMIT 1", 'key').first
      expect(row[:created_at]).not_to be_nil
      expect(row[:data]).to eq('{"foo":"bar"}')
    end
    it 'returns nil for a duplicated task' do
      expect(db.submit('key', '{"foo":"bar"}')).to be true
      expect(db.submit('key', '{"foo":"bar"}')).to be_nil
    end
  end

  context '#cancel' do
    let (:key){ 'key' }
    context 'have the task' do
      before do
        db.submit(key, '{}')
      end
      it 'returns true' do
        expect(db.cancel(key)).to be true
        row = db.db.fetch("SELECT created_at FROM `#{table}` WHERE id=? LIMIT 1", key).first
        expect(row[:created_at]).to be_nil
      end
    end
    context 'already canceled' do
      it 'returns false' do
        expect(db.cancel(key)).to be false
      end
    end
  end

  context '#connect' do
    context 'normal' do
      it 'returns nil' do
        expect(db.__send__(:connect){ }).to be_nil
      end
    end
    context 'error' do
      it 'returns block result' do
        expect(RuntimeError).to receive(:new).exactly(Backend::RDBBackend::MAX_RETRY).and_call_original
        allow(STDERR).to receive(:puts)
        allow(db).to receive(:sleep)
        expect do
          db.__send__(:connect) do
            raise RuntimeError.new('try restarting transaction')
          end
        end.to raise_error(RuntimeError)
      end
    end
  end
end

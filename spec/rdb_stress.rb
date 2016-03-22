require 'spec_helper'
require 'perfectqueue/backend/rdb_compat'
require 'logger'

# run this with `bundle exec rake spec SPEC_OPTS="-fd" SPEC=spec/rdb_stress.rb`

describe Backend::RDBCompatBackend do
  let (:now){ Time.now.to_i }
  let (:client){ double('client') }
  let (:table){ 'test_queues' }
  let (:config){ {
    url: 'mysql2://root:@localhost/perfectqueue_test',
    table: table,
    disable_resource_limit: true,
  } }
  let (:db) do
    d = Backend::RDBCompatBackend.new(client, config)
    s = d.db
    s.tables.each{|t| s.drop_table(t) }
    d.init_database({})
    d
  end

  context '#acquire' do
    let (:task_token){ Backend::RDBCompatBackend::Token.new(key) }
    let (:alive_time){ 42 }
    let (:max_acquire){ 10 }

    context 'some tasks' do
      before do
        sql = nil
        bucket_size = 200000
        600_000.times do |i|
          if i % bucket_size == 0
            sql = 'INSERT `test_queues` (id, timeout, data, created_at, resource) VALUES'
          end
          t = now - 600 + i/1000
          sql << "(UUID(),#{t},TO_BASE64(RANDOM_BYTES(540)),#{t},NULL),"
          if i % bucket_size == bucket_size - 1
            db.db.run sql.chop!
          end
        end
        db.db.loggers << Logger.new($stderr)
        db.db.sql_log_level = :debug
      end
      it 'returns a task' do
        #db.instance_variable_set(:@cleanup_interval_count, 0)
        #expect(db.db.instance_variable_get(:@default_dataset)).to receive(:delete).and_call_original
        ary = db.acquire(alive_time, max_acquire, {})
        expect(ary).to be_an_instance_of(Array)
        expect(ary.size).to eq(10)
        expect(ary[0]).to be_an_instance_of(AcquiredTask)
      end
    end

    context 'very large jobs' do
      before do
        sql = nil
        sql = 'INSERT `test_queues` (id, timeout, data, created_at, resource) VALUES'
        data = %<UNCOMPRESS(UNCOMPRESS(FROM_BASE64('6B8AAHic7c6xCYNQFEDRh8HGAawjmAmEOE7WsPziGoJ1xnAJLbJCVgiJbpBOkHOqW96IFN34nvvYpOvyuZXPIgAAAICTS6/hku1RfR9tffQNAAAA8Icxb+7r9AO74A1h')))>
        200.times do |i|
          t = now - 600 + i/1000
          sql << "(UUID(),#{t},#{data},#{t},NULL),"
        end
        db.db.run sql.chop!
        db.db.loggers << Logger.new($stderr)
        db.db.sql_log_level = :debug
      end
      it 'returns a task' do
        #db.instance_variable_set(:@cleanup_interval_count, 0)
        #expect(db.db.instance_variable_get(:@default_dataset)).to receive(:delete).and_call_original
        ary = db.acquire(alive_time, max_acquire, {})
        expect(ary).to be_an_instance_of(Array)
        expect(ary.size).to eq(10)
        expect(ary[0]).to be_an_instance_of(AcquiredTask)
      end
    end
  end
end

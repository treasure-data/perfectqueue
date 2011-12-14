require File.dirname(__FILE__)+'/test_helper'

class BackendTest < Test::Unit::TestCase
  TIMEOUT = 10
  DB_PATH = File.dirname(__FILE__)+'/test.db'
  DB_URI = "sqlite://#{DB_PATH}"

  def clean_backend
    @key_prefix = "test-#{"%08x"%rand(2**32)}-"
    db = open_backend
    db.list {|id,created_at,data,timeout|
      db.cancel(id)
    }
    FileUtils.rm_f DB_PATH
  end

  def open_backend
    #PerfectQueue::SimpleDBBackend.new(ENV['AWS_ACCESS_KEY_ID'], ENV['AWS_SECRET_ACCESS_KEY'], 'perfectqueue-test-1').use_consistent_read
    db = PerfectQueue::RDBBackend.new(DB_URI, "perfectdb_test")
    db.create_tables
    db
  end

  it 'acquire' do
    clean_backend

    db1 = open_backend
    db2 = open_backend
    db3 = open_backend

    time = Time.now.to_i

    ok = db1.submit(@key_prefix+'test1', 'data1', time)
    assert_equal true, ok

    token, task = db2.acquire(time+TIMEOUT)
    assert_not_equal nil, task
    assert_equal @key_prefix+'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data

    token_, task_ = db3.acquire(time+TIMEOUT)
    assert_equal nil, token_
  end

  it 'finish' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    ok = db1.submit(@key_prefix+'test1', 'data1', time)
    assert_equal true, ok

    token, task = db1.acquire(time+TIMEOUT)
    assert_not_equal nil, task

    ok = db1.finish(token)
    assert_equal true, ok

    token_, task_ = db2.acquire(time+TIMEOUT)
    assert_equal nil, token_

    ok = db1.finish(token)
    assert_equal false, ok
  end

  it 'canceled' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    ok = db1.submit(@key_prefix+'test1', 'data1', time)
    assert_equal true, ok

    token, task = db1.acquire(time+TIMEOUT)
    assert_not_equal nil, task

    ok = db1.cancel(task.id)
    assert_equal true, ok

    token_, task_ = db2.acquire(time+TIMEOUT)
    assert_equal nil, token_

    assert_raise(PerfectQueue::CanceledError) do
      db1.update(token, time+TIMEOUT)
    end

    ok = db1.cancel(task.id)
    assert_equal false, ok
  end

  it 'order' do
    clean_backend

    db1 = open_backend
    db2 = open_backend
    db3 = open_backend

    time = Time.now.to_i

    ok = db1.submit(@key_prefix+'test1', 'data1', time)
    assert_equal true, ok

    ok = db1.submit(@key_prefix+'test2', 'data2', time-1)
    assert_equal true, ok

    ok = db1.submit(@key_prefix+'test3', 'data3', time+1)
    assert_equal true, ok

    token, task = db2.acquire(time+TIMEOUT, time+1)
    assert_not_equal nil, task
    assert_equal @key_prefix+'test2', task.id
    assert_equal time-1, task.created_at
    assert_equal 'data2', task.data

    token, task = db2.acquire(time+TIMEOUT, time+1)
    assert_not_equal nil, task
    assert_equal @key_prefix+'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data

    token, task = db2.acquire(time+TIMEOUT, time+1)
    assert_not_equal nil, task
    assert_equal @key_prefix+'test3', task.id
    assert_equal time+1, task.created_at
    assert_equal 'data3', task.data
  end

  it 'timeout' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    ok = db1.submit(@key_prefix+'test1', 'data1', time)
    assert_equal true, ok

    token, task = db1.acquire(time+TIMEOUT)
    assert_not_equal nil, task
    assert_equal @key_prefix+'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data

    token, task = db2.acquire(time+TIMEOUT*2, time+TIMEOUT)
    assert_not_equal nil, task
    assert_equal @key_prefix+'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data
  end

  it 'extend' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    ok = db1.submit(@key_prefix+'test1', 'data1', time)
    assert_equal true, ok

    token, task = db1.acquire(time+TIMEOUT)
    assert_not_equal nil, task

    assert_nothing_raised do
      db1.update(token, time+TIMEOUT)
    end

    token_, task_ = db2.acquire(time+TIMEOUT, time)
    assert_equal nil, token_

    token, task = db2.acquire(time+TIMEOUT*2, time+TIMEOUT)
    assert_not_equal nil, task
    assert_equal @key_prefix+'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data
  end

  it 'release' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    ok = db1.submit(@key_prefix+'test1', 'data1', time)
    assert_equal true, ok

    token, task = db1.acquire(time+TIMEOUT)
    assert_not_equal nil, task

    assert_nothing_raised do
      db1.update(token, time+TIMEOUT)
    end

    token_, task_ = db2.acquire(time+TIMEOUT, time)
    assert_equal nil, token_

    assert_nothing_raised do
      db1.update(token, time)
    end

    token, task = db2.acquire(time+TIMEOUT, time)
    assert_not_equal nil, task
    assert_equal @key_prefix+'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data
  end
end


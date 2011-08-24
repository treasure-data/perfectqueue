require File.dirname(__FILE__)+'/test_helper'

class BackendTest < Test::Unit::TestCase
  TIMEOUT = 10
  DB_PATH = File.dirname(__FILE__)+'/test.db'
  DB_URI = "sqlite://#{DB_PATH}"

  def clean_backend
    db = open_backend
    db.list {|id,created_at,data,timeout|
      db.cancel(id)
    }
    FileUtils.rm_f DB_PATH
  end

  def open_backend
    PerfectQueue::RDBBackend.new(DB_URI, "perfectdb_test")
  end

  it 'acquire' do
    clean_backend

    db1 = open_backend
    db2 = open_backend
    db3 = open_backend

    time = Time.now.to_i

    assert_nothing_raised do
      db1.submit('test1', 'data1', time)
    end

    token, task = db2.acquire(time+TIMEOUT)
    assert_not_equal nil, task
    assert_equal 'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data

    token_, task = db3.acquire(time+TIMEOUT)
    assert_equal nil, token_
  end

  it 'finish' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)
    token, task = db1.acquire(time+TIMEOUT)
    assert_not_equal nil, task

    db1.finish(token)

    token_, task = db2.acquire(time+TIMEOUT)
    assert_equal nil, token_
  end

  it 'canceled' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)
    token, task = db1.acquire(time+TIMEOUT)
    assert_not_equal nil, task

    db1.cancel(task.id)

    token_, task = db2.acquire(time+TIMEOUT)
    assert_equal nil, token_

    assert_raise(PerfectQueue::CanceledError) do
      db1.update(token, time+TIMEOUT)
    end
  end

  it 'order' do
    clean_backend

    db1 = open_backend
    db2 = open_backend
    db3 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)
    db1.submit('test2', 'data2', time-1)
    db1.submit('test3', 'data3', time+1)

    token, task = db2.acquire(time+TIMEOUT, time+1)
    assert_not_equal nil, task
    assert_equal 'test2', task.id
    assert_equal time-1, task.created_at
    assert_equal 'data2', task.data

    token, task = db2.acquire(time+TIMEOUT, time+1)
    assert_not_equal nil, task
    assert_equal 'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data

    token, task = db2.acquire(time+TIMEOUT, time+1)
    assert_not_equal nil, task
    assert_equal 'test3', task.id
    assert_equal time+1, task.created_at
    assert_equal 'data3', task.data
  end

  it 'timeout' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)

    token, task = db1.acquire(time+TIMEOUT)
    assert_not_equal nil, task
    assert_equal 'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data

    token, task = db2.acquire(time+TIMEOUT*2, time+TIMEOUT)
    assert_not_equal nil, task
    assert_equal 'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data
  end

  it 'extend' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)

    token, task = db1.acquire(time+TIMEOUT)
    assert_not_equal nil, task

    assert_nothing_raised do
      db1.update(token, time+TIMEOUT)
    end

    token_, task = db2.acquire(time+TIMEOUT, time)
    assert_equal nil, token_

    token, task = db2.acquire(time+TIMEOUT*2, time+TIMEOUT)
    assert_not_equal nil, task
    assert_equal 'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data
  end

  it 'release' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)

    token, task = db1.acquire(time+TIMEOUT)
    assert_not_equal nil, task

    assert_nothing_raised do
      db1.update(token, time+TIMEOUT)
    end

    token_, task = db2.acquire(time+TIMEOUT, time)
    assert_equal nil, token_

    assert_nothing_raised do
      db1.update(token, time)
    end

    token, task = db2.acquire(time+TIMEOUT, time)
    assert_not_equal nil, task
    assert_equal 'test1', task.id
    assert_equal time, task.created_at
    assert_equal 'data1', task.data
  end
end


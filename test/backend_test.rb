require File.dirname(__FILE__)+'/test_helper'

class BackendTest < Test::Unit::TestCase
  TIMEOUT = 10
  DB_PATH = File.dirname(__FILE__)+'/test.db'
  DB_URI = "sqlite://#{DB_PATH}"

  def clean_backend
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

    id, created_at, data = db2.acquire(time+TIMEOUT)
    assert_equal 'test1', id
    assert_equal time, created_at
    assert_equal 'data1', data

    id_, created_at, data = db3.acquire(time+TIMEOUT)
    assert_equal nil, id_
  end

  it 'finish' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)
    id, created_at, data = db1.acquire(time+TIMEOUT)

    db1.finish(id)

    id_, created_at, data = db2.acquire(time+TIMEOUT)
    assert_equal nil, id_
  end

  it 'canceled' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)
    id, created_at, data = db1.acquire(time+TIMEOUT)

    db1.cancel(id)

    id_, created_at, data = db2.acquire(time+TIMEOUT)
    assert_equal nil, id_

    assert_raise(PerfectQueue::CanceledError) do
      db1.update(id, time+TIMEOUT)
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

    id, created_at, data = db2.acquire(time+TIMEOUT, time+1)
    assert_equal 'test2', id
    assert_equal time-1, created_at
    assert_equal 'data2', data

    id, created_at, data = db2.acquire(time+TIMEOUT, time+1)
    assert_equal 'test1', id
    assert_equal time, created_at
    assert_equal 'data1', data

    id, created_at, data = db2.acquire(time+TIMEOUT, time+1)
    assert_equal 'test3', id
    assert_equal time+1, created_at
    assert_equal 'data3', data
  end

  it 'timeout' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)

    id, created_at, data = db1.acquire(time+TIMEOUT)
    assert_equal 'test1', id
    assert_equal time, created_at
    assert_equal 'data1', data

    id, created_at, data = db2.acquire(time+TIMEOUT*2, time+TIMEOUT)
    assert_equal 'test1', id
    assert_equal time, created_at
    assert_equal 'data1', data
  end

  it 'extend' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)

    id, created_at, data = db1.acquire(time+TIMEOUT)

    assert_nothing_raised do
      db1.update(id, time+TIMEOUT)
    end

    id_, created_at, data = db2.acquire(time+TIMEOUT, time)
    assert_equal nil, id_

    id, created_at, data = db2.acquire(time+TIMEOUT*2, time+TIMEOUT)
    assert_equal 'test1', id
    assert_equal time, created_at
    assert_equal 'data1', data
  end

  it 'release' do
    clean_backend

    db1 = open_backend
    db2 = open_backend

    time = Time.now.to_i

    db1.submit('test1', 'data1', time)

    id, created_at, data = db1.acquire(time+TIMEOUT)

    assert_nothing_raised do
      db1.update(id, time+TIMEOUT)
    end

    id_, created_at, data = db2.acquire(time+TIMEOUT, time)
    assert_equal nil, id_

    assert_nothing_raised do
      db1.update(id, time)
    end

    id, created_at, data = db2.acquire(time+TIMEOUT, time)
    assert_equal 'test1', id
    assert_equal time, created_at
    assert_equal 'data1', data
  end
end


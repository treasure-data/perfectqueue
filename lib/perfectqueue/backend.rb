
module PerfectQueue


class Task
  def initialize(id, created_at, data, resource=nil)
    @id = id
    @created_at = created_at
    @data = data
    @resource = resource
  end

  attr_reader :id, :created_at, :data, :resource
end


class CanceledError < RuntimeError
end


class Backend
  # => list {|id,created_at,data,timeout| ... }
  def list(&block)
  end

  # => token, task
  def acquire(timeout, now=Time.now.to_i)
  end

  # => true (success) or false (canceled)
  def finish(token, delete_timeout=3600, now=Time.now.to_i)
  end

  # => nil
  def update(token, timeout)
  end

  # => true (success) or false (not found, canceled or finished)
  def cancel(id, delete_timeout=3600, now=Time.now.to_i)
  end

  # => true (success) or nil (already exists)
  def submit(id, data, time=Time.now.to_i, resource=nil)
  end

  def close
  end
end


end


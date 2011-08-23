
module PerfectQueue


class CanceledError < RuntimeError
end


class Backend
  # => list {|id,created_at,data,timeout| ... }
  def list(&block)
  end

  # => id, created_at, data:map
  def acquire(timeout, now=Time.now.to_i)
  end

  # => true (success) or false (canceled)
  def finish(id)
  end

  # => nil
  def update(id, timeout)
  end

  # => true (success) or false (not found, canceled or finished)
  def cancel(id)
  end

  # => nil
  def submit(id, data, time=Time.now.to_i)
  end

  def close
  end
end


end


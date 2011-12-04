
module PerfectQueue


class NullBackend < Backend
  def list(&block)
    nil
  end

  def acquire(timeout, now=Time.now.to_i)
    nil
  end

  def finish(token, delete_timeout=3600, now=Time.now.to_i)
    false
  end

  def update(token, timeout)
    nil
  end

  def cancel(id, delete_timeout=3600, now=Time.now.to_i)
    false
  end

  def submit(id, data, time=Time.now.to_i)
    nil
  end
end


end



module PerfectQueue


class Task
  def initialize(id, data)
    @id = id
    @data = data
  end

  attr_reader :id, :data
end


class MonitorThread
  def initialize(engine, conf)
    @engine = engine
    @log = @engine.log
    @backend = engine.backend

    @timeout = conf[:timeout] || 30
    @heartbeat_interval = conf[:heartbeat_interval] || @timeout*3/4
    @kill_timeout = conf[:kill_timeout] || @timeout*10
    @kill_interval = conf[:kill_interval] || 60
    @retry_wait = conf[:retry_wait] || nil

    @id = nil
    @heartbeat_time = nil
    @kill_time = nil
    @kill_proc = nil
    @canceled = false
    @mutex = Mutex.new
    @cond = ConditionVariable.new
  end

  def start
    @log.debug "running worker."
    @thread = Thread.new(&method(:run))
  end

  def run
    until @engine.finished?
      @mutex.synchronize {
        while true
          return if @engine.finished?
          break if @id
          @cond.wait(@mutex)
        end
      }
      while true
        sleep 1
        @mutex.synchronize {
          return if @engine.finished?
          break unless @id
          now = Time.now.to_i
          try_extend(now)
          try_kill(now)
        }
      end
    end
  rescue
    @engine.stop($!)
  end

  def try_extend(now)
    if now >= @heartbeat_time && !@canceled
      @log.debug "extending timeout=#{now+@timeout} id=#{@id}"
      begin
        @backend.update(@id, now+@timeout)
      rescue CanceledError
        @log.info "task id=#{@id} is canceled."
        @canceled = true
        @kill_time = now
      end
      @heartbeat_time = now + @heartbeat_interval
    end
  end

  def try_kill(now)
    if now >= @kill_time
      kill!
      @kill_time = now + @kill_interval
    end
  end

  def kill!
    if @kill_proc
      @log.info "killing #{@id}..."
      @kill_proc.call rescue nil
    end
  end

  def stop
    @mutex.synchronize {
      @cond.broadcast
    }
  end

  def shutdown
    @thread.join
  end

  def set(id)
    @mutex.synchronize {
      now = Time.now.to_i
      @id = id
      @heartbeat_time = now + @heartbeat_interval
      @kill_time = now + @kill_timeout
      @kill_proc = nil
      @canceled = false
      @cond.broadcast
    }
  end

  def set_kill_proc(kill_proc)
    @kill_proc = kill_proc
  end

  def reset(success)
    @mutex.synchronize {
      if success
        @backend.finish(@id)
      elsif @retry_wait && !@canceled
        begin
          @backend.update(@id, Time.now.to_i+@retry_wait)
        rescue
          # ignore CanceledError
        end
      end
      @id = nil
    }
  end
end


class Worker
  def initialize(engine, conf)
    @engine = engine
    @log = @engine.log

    @run_class = conf[:run_class]
    @monitor = MonitorThread.new(engine, conf)

    @id = nil
    @data = nil
    @mutex = Mutex.new
    @cond = ConditionVariable.new
  end

  def start
    @thread = Thread.new(&method(:run))
    @monitor.start
  end

  def run
    while true
      @mutex.synchronize {
        while true
          return if @engine.finished?
          break if @id
          @cond.wait(@mutex)
        end
      }
      process(@id, @data)
    end
  rescue
    @engine.stop($!)
  end

  def process(id, data)
    @log.info "processing task id=#{id}"

    @monitor.set(id)
    success = false
    begin
      task = Task.new(id, data)
      run = @run_class.new(task)

      if run.respond_to?(:kill)
        @monitor.set_kill_proc run.method(:kill)
      end

      run.run

      @log.info "finished id=#{id}"
      success = true

    rescue
      @log.info "failed id=#{id}: #{$!}"

    ensure
      @monitor.reset(success)
    end

  ensure
    @id = nil
    @engine.release_worker(self)
  end

  def stop
    submit(nil, nil)
    @monitor.stop
  end

  def shutdown
    @monitor.shutdown
    @thread.join
  end

  def submit(id, data)
    @mutex.synchronize {
      @id = id
      @data = data
      @cond.broadcast
    }
  end
end


end



module PerfectQueue


class MonitorThread
  def initialize(engine, conf)
    @engine = engine
    @log = @engine.log
    @backend = engine.backend
    @finished = false

    @timeout = conf[:timeout] || 600
    @heartbeat_interval = conf[:heartbeat_interval] || @timeout*3/4
    @kill_timeout = conf[:kill_timeout] || @timeout*10
    @kill_interval = conf[:kill_interval] || 60
    @retry_wait = conf[:retry_wait] || nil
    @delete_wait = conf[:delete_wait] || 3600

    @token = nil
    @heartbeat_time = nil
    @kill_time = nil
    @kill_proc = nil
    @canceled = false
    @mutex = Mutex.new
    @cond = ConditionVariable.new
  end

  def start
    @thread = Thread.new(&method(:run))
  end

  def run
    until @finished
      @mutex.synchronize {
        while true
          return if @finished
          break if @token
          @cond.wait(@mutex)
        end
      }
      process
    end
  rescue
    @engine.stop($!)
  end

  def process
    while true
      sleep 1
      @mutex.synchronize {
        return if @finished
        return unless @token
        now = Time.now.to_i
        try_extend(now)
        try_kill(now)
      }
    end
  end

  def try_extend(now)
    if now >= @heartbeat_time && !@canceled
      @log.debug "extending timeout=#{now+@timeout} id=#{@task_id}"
      begin
        @backend.update(@token, now+@timeout)
      rescue CanceledError
        @log.info "task id=#{@task_id} is canceled."
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
      @log.info "killing id=#{@task_id}..."
      begin
        @kill_proc.call
      rescue
        @log.info "kill failed id=#{@task_id}: #{$!.class}: #{$!}"
        $!.backtrace.each {|bt|
          @log.debug "  #{bt}"
        }
      end
    end
  end

  def stop
    @mutex.synchronize {
      @finished = true
      @cond.broadcast
    }
  end

  def shutdown
    @thread.join
  end

  def set(token, task_id)
    @mutex.synchronize {
      now = Time.now.to_i
      @token = token
      @task_id = task_id
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
        @backend.finish(@token, @delete_wait)
      elsif @retry_wait && !@canceled
        begin
          @backend.update(@token, Time.now.to_i+@retry_wait)
        rescue
          # ignore CanceledError
        end
      end
      @token = nil
    }
  end
end


class Worker
  def initialize(engine, conf)
    @engine = engine
    @log = @engine.log

    @run_class = conf[:run_class]
    @monitor = MonitorThread.new(engine, conf)

    @token = nil
    @task = nil
    @mutex = Mutex.new
    @cond = ConditionVariable.new
  end

  def start
    @log.debug "running worker."
    @thread = Thread.new(&method(:run))
  end

  def run
    @monitor.start
    begin
      while true
        @mutex.synchronize {
          while true
            return if @engine.finished?
            break if @token
            @cond.wait(@mutex)
          end
        }
        begin
          process(@token, @task)
        ensure
          @token = nil
          @engine.release_worker(self)
        end
      end
    ensure
      @monitor.stop
    end
  rescue
    @engine.stop($!)
  end

  def process(token, task)
    @log.info "processing task id=#{task.id}"

    @monitor.set(token, task.id)
    success = false
    begin
      run = @run_class.new(task)

      if run.respond_to?(:kill)
        @monitor.set_kill_proc run.method(:kill)
      end

      run.run

      @log.info "finished id=#{task.id}"
      success = true

    rescue
      @log.info "failed id=#{task.id}: #{$!.class}: #{$!}"
      $!.backtrace.each {|bt|
        @log.debug "  #{bt}"
      }

    ensure
      @monitor.reset(success)
    end
  end

  def stop
    submit(nil, nil)
  end

  def shutdown
    @monitor.shutdown
    @thread.join
  end

  def submit(token, task)
    @mutex.synchronize {
      @token = token
      @task = task
      @cond.broadcast
    }
  end
end


end


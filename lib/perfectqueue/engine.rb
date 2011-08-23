
module PerfectQueue


class Engine
  def initialize(backend, log, conf)
    @backend = backend
    @log = log

    @timeout = conf[:timeout]
    @poll_interval = conf[:poll_interval] || 1
    @expire = conf[:expire] || 345600

    num_workers = conf[:workers] || 1
    @workers = (1..num_workers).map {
      Worker.new(self, conf)
    }
    @available_workers = @workers.dup

    @finished = false
    @error = nil

    @mutex = Mutex.new
    @cond = ConditionVariable.new
  end

  attr_reader :backend
  attr_reader :log
  attr_reader :error

  def finished?
    @finished
  end

  def run
    @workers.each {|w|
      w.start
    }

    until finished?
      w = acquire_worker
      next unless w
      begin

        until finished?
          id, created_at, data = @backend.acquire(Time.now.to_i+@timeout)

          unless id
            sleep @poll_interval
            next
          end
          if created_at > Time.now.to_i+@expire
            @log.warn "canceling expired task id=#{id}"
            @backend.cancel(id)
            next
          end

          @log.info "acquired task id=#{id}"
          w.submit(id, data)
          w = nil
          break
        end

      ensure
        release_worker(w) if w
      end
    end
  ensure
    @finished = true
  end

  def stop(error=nil)
    @finished = true
    @error = error
    @workers.each {|w|
      w.stop
    }

    if error
      log.error error.to_s
      error.backtrace.each {|x|
        log.error "  #{x}"
      }
    end
  end

  def shutdown
    @finished = true
    @workers.each {|w|
      w.shutdown
    }
  end

  def acquire_worker
    @mutex.synchronize {
      while @available_workers.empty?
        return nil if finished?
        @cond.wait(@mutex)
      end
      return @available_workers.pop
    }
  end

  def release_worker(worker)
    @mutex.synchronize {
      @available_workers.push worker
      if @available_workers.size == 1
        @cond.broadcast
      end
    }
  end
end


class ExecRunner
  def initialize(cmd, task)
    @cmd = cmd
    @task = task
    @iobuf = ''
    @pid = nil
    @next_kill = :TERM
  end

  def run
    cmdline = "#{@cmd} #{Shellwords.escape(@task.id)}"
    IO.popen(cmdline, "r+") {|io|
      @pid = io.pid
      io.write(@task.data) rescue nil
      io.close_write
      begin
        while true
          io.sysread(1024, @iobuf)
          print @iobuf
        end
      rescue EOFError
      end
    }
    if $?.to_i != 0
      raise "Command failed"
    end
  end

  def kill
    Process.kill(@next_kill, @pid)
    @next_kill = :KILL
  end
end


end


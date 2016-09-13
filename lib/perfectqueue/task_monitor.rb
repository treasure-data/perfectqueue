#
# PerfectQueue
#
# Copyright (C) 2012-2013 Sadayuki Furuhashi
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

module PerfectQueue

  class TaskMonitor
    def initialize(config, child_heartbeat=nil, force_stop=nil)
      @config = config
      @log = config[:logger]
      @child_heartbeat = child_heartbeat || Proc.new {}
      @force_stop = force_stop || Proc.new {}

      @child_heartbeat_interval = (@config[:child_heartbeat_interval] || 2).to_i
      @task_heartbeat_interval = (@config[:task_heartbeat_interval] || 2).to_i
      @last_child_heartbeat = Time.now.to_i
      @last_task_heartbeat = Time.now.to_i

      @task = nil

      @mutex = Monitor.new  # support recursive lock
      @cond = @mutex.new_cond
      @finished = false
    end

    def start
      @thread = Thread.new(&method(:run))
    end

    def stop
      @finished = true
      @mutex.synchronize {
        @cond.broadcast
      }
    end

    def join
      @thread.join
    end

    def set_task(task, runner)
      task.extend(TaskMonitorHook)
      task.log = @log
      task.task_monitor = self
      task.runner = runner
      @mutex.synchronize {
        @task = task
        @last_task_heartbeat = @task.timeout.to_i
      }
    end

    def stop_task(immediate)
      if immediate
        kill_task ImmediateProcessStopError.new('immediate stop requested')
      else
        kill_task GracefulProcessStopError.new('graceful stop requested')
      end
    end

    def kill_task(reason)
      @mutex.synchronize {
        if task = @task
          begin
            task.runner.kill(reason)  # may recursive lock
          rescue
            @log.error "failed to kill task: #{$!.class}: #{$!}"
            $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
            raise # force exit
          end
        end
      }
    end

    # callback
    def task_finished(task, &block)
      @mutex.synchronize {
        ret = block.call if block  # TODO is this ought to be synchronized?
        if task == @task
          @task = nil
        end
        ret
      }
    end

    # callback
    def external_task_heartbeat(task, &block)
      @mutex.synchronize {
        if task == @task
          ret = block.call if block
          @last_task_heartbeat = Time.now.to_i
        end
        ret
      }
    end

    def run
      @mutex.synchronize {
        now = Time.now.to_i

        until @finished
          next_child_heartbeat = @last_child_heartbeat + @child_heartbeat_interval

          if @task
            next_task_heartbeat = @last_task_heartbeat + @task_heartbeat_interval
            next_time = [next_child_heartbeat, next_task_heartbeat].min
          else
            next_task_heartbeat = nil
            next_time = next_child_heartbeat
          end

          next_wait = next_time - now
          @cond.wait(next_wait) if next_wait > 0

          now = Time.now.to_i
          if @task && next_task_heartbeat && next_task_heartbeat <= now
            task_heartbeat
            @last_task_heartbeat = now
          end

          if next_child_heartbeat <= now
            @child_heartbeat.call  # will recursive lock
            @last_child_heartbeat = now
          end
        end
      }
    rescue
      @log.error "Unknown error #{$!.class}: #{$!}"
      $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
      @force_stop.call
    end

    private
    def task_heartbeat
      v = @task.heartbeat!(last_heartbeat: @last_task_heartbeat)
      @task.attributes[:timeout] = v
      v
    rescue
      # finished, preempted, etc.
      kill_task($!)
    end
  end

  module TaskMonitorHook
    attr_accessor :log
    attr_accessor :task_monitor
    attr_accessor :runner

    def finish!(*args, &block)
      @log.info "finished task=#{self.key}" if @log
      @task_monitor.task_finished(self) {
        super(*args, &block)
      }
    end

    def release!(*args, &block)
      @log.info "release task=#{self.key}" if @log
      @task_monitor.task_finished(self) {
        super(*args, &block)
      }
    end

    def retry!(*args, &block)
      @log.info "retry task=#{self.key}" if @log
      @task_monitor.task_finished(self) {
        super(*args, &block)
      }
    end

    def update_data!(hash)
      @log.info "update data #{hash.inspect} task=#{self.key}" if @log
      @task_monitor.external_task_heartbeat(self) {
        super(hash)
      }
    end
  end

end


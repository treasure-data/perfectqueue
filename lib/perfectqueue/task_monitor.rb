#
# PerfectQueue
#
# Copyright (C) 2012 FURUHASHI Sadayuki
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
    def initialize(config, child_heartbeat=nil)
      @config = config
      @log = config[:logger]
      @child_heartbeat = child_heartbeat || Proc.new {}

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
      task.task_monitor = self
      task.runner = runner
      @mutex.synchronize {
        @task = task
        @last_task_heartbeat = Time.now.to_i
        @heartbeat_message = nil
      }
    end

    # callback
    def set_heartbeat_message(task, message)
      @mutex.synchronize {
        if task == @task
          @heartbeat_message = message
        end
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
        ret = block.call if block
        if task == @task
          @task = nil
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
            next_time = next_child_heartbeat
          end

          next_wait = [1, next_time - now].max
          @cond.wait(next_wait) if next_wait > 0  # TODO timeout doesn't work?

          now = Time.now.to_i
          if @task && next_task_heartbeat && now <= next_task_heartbeat
            task_heartbeat
            @last_task_heartbeat = now
          end

          if now <= next_child_heartbeat
            @child_heartbeat.call  # will recursive lock
            @last_child_heartbeat = now
          end
        end
      }
    rescue
      @log.error "Unknown error #{$!.class}: #{$!}. Exiting worker pid=#{Process.pid}"
      $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
    end

    private
    def task_heartbeat
      @task.heartbeat! :message => @heartbeat_message
      @heartbeat_message = nil
    rescue TaskError
      # finished, cancel_requested, preempted, etc.
      kill_task($!)
    end
  end

  module TaskMonitorHook
    attr_accessor :task_monitor
    attr_accessor :runner

    def heartbeat_message=(message)
      @heartbeat_message = message
      @task_monitor.set_heartbeat_message(self, message)
      message
    end

    attr_reader :heartbeat_message

    def finish!(*args, &block)
      @task_monitor.task_finished(self) {
        super(*args, &block)
      }
    end

    def release!(*args, &block)
      @task_monitor.task_finished(self) {
        super(*args, &block)
      }
    end

    def retry!(*args, &block)
      @task_monitor.task_finished(self) {
        super(*args, &block)
      }
    end

    def cancel_request!(*args, &block)
      @task_monitor.task_finished(self) {
        super(*args, &block)
      }
    end
  end

end


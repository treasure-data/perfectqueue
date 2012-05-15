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
  module Multiprocess

    class ChildProcess
      def initialize(config, wpipe, runner)
        @config = config
        @runner = runner

        @log = config[:logger]

        @poll_interval = @config[:poll_interval] || 1.0

        @finished = false
        @mutex = Mutex.new
        @cond = ConditionVariable.new

        @tm = TaskMonitor.new(self, wpipe, @runner)

        install_signal_handlers
      end

      attr_reader :config
      attr_reader :log

      def main(after_fork, before_child_end)
        @tm.start
        begin
          call_user_method(&after_fork) if after_fork

          PerfectQueue.open(@config) {|queue|
            @client = queue.client

            until @finished
              task = queue.poll
              if task
                call_user_method { process(task) }
              else
                # TODO @cond.wait(@mutex, @poll_interval)
                #      stop want to interrupt
                sleep @poll_interval
              end
            end

            call_user_method(&before_child_end) if before_child_end

            @client = nil
          }
        ensure
          @tm.stop(true)
          @tm.join
        end
      rescue
        @log.error "Unknown error #{$!.class}: #{$!}. Exiting worker pid=#{Process.pid}"
        $!.backtrace.each {|x| @log.error "  #{x}" }
        ChildProcess.force_exit!(nil)
      end

      def stop(graceful=true)
        if graceful
          @log.debug "wait and stop pid=#{Process.pid}"
        else
          @log.debug "kill and stop pid=#{Process.pid}"
        end
        @finished = true
        @tm.stop(graceful)
      end

      def call_user_method(&block)
        begin
          block.call
        rescue
          @log.error "callback handler raises: #{$!.class}: #{$!}. Exiting worker pid=#{Process.pid}"
          $!.backtrace.each {|x| @log.error "  #{x}" }
          force_exit!
        end
      end

      def force_exit!
        begin
          @client.close if @client
        rescue
        end
        Process.kill(:KILL, Process.pid)
        exit! 137
      end

      private
      def process(task)
        @log.info "acquired task: #{task.inspect}"
        @tm.set_task(task)
        begin
          @runner.run(task)
        ensure
          @tm.finish_task(task)
        end
      end

      def install_signal_handlers
        trap :TERM do
          stop(true)
        end
        trap :INT do
          stop(true)
        end

        trap :QUIT do
          stop(false)
        end

        trap :USR1 do
          stop(true)
        end

        trap :USR2 do
          stop(false)
        end

        trap :HUP do
          stop(true)
        end

        trap :WINCH do
          stop(false)
        end

        trap :SIGCONT do
        end
      end
    end


    class TaskMonitor
      def initialize(cp, wpipe, runner)
        @cp = cp
        @config = cp.config
        @log = cp.log

        @runner = runner

        @child_heartbeat_interval = (@config[:child_heartbeat_interval] || 1).to_i
        @task_heartbeat_interval = (@config[:task_heartbeat_interval] || 1).to_i
        @last_child_heartbeat = Time.now.to_i
        @last_task_heartbeat = Time.now.to_i

        @running_task = nil
        @finished_task = nil

        @wpipe = wpipe
        @wpipe.sync = true

        @mutex = Mutex.new
        @cond = ConditionVariable.new
        @finished = false
      end

      def start
        @thread = Thread.new(&method(:main))
      end

      def stop(graceful)
        @mutex.synchronize {
          if !graceful && @running_task
            @runner.kill(ProcessStopError)
          end
          unless @finished
            @finished = true
            @cond.broadcast
          end
        }
      end

      def join
        @thread.join
      end

      def set_task(task)
        task.extend(TaskMonitorHook)
        task.task_monitor = self
        @mutex.synchronize {
          @running_task = task
          @last_task_heartbeat = Time.now.to_i
          @heartbeat_message = nil
        }
      end

      def set_heartbeat_message(task, message)
        @mutex.synchronize {
          if @running_task == task
            @heartbeat_message = message
          end
        }
      end

      def finish_task(task, &block)
        @mutex.synchronize {
          ret = block.call if block
          if @running_task == task
            @running_task = nil
          end
          ret
        }
      end

      private
      def main
        @mutex.synchronize {
          now = Time.now.to_i

          until @finished
            next_child_heartbeat = @last_child_heartbeat + @child_heartbeat_interval

            if @running_task
              next_task_heartbeat = @last_task_heartbeat + @task_heartbeat_interval
              next_time = [next_child_heartbeat, next_task_heartbeat].min
            else
              next_time = next_child_heartbeat
            end

            next_wait = [1, next_time - now].min
            @cond.wait(@mutex, next_wait) if next_wait > 0

            now = Time.now.to_i
            if @running_task && next_task_heartbeat && now <= next_task_heartbeat
              task_heartbeat
              @last_task_heartbeat = now
            end

            if now <= next_child_heartbeat
              child_heartbeat
              @last_child_heartbeat = now
            end
          end
        }
      rescue
        @log.error "Unknown error #{$!.class}: #{$!}. Exiting worker pid=#{Process.pid}"
        $!.backtrace.each {|x| @log.error "  #{x}" }
        ChildProcess.force_exit!
      end

      HEARTBEAT_PACKET = [0].pack('C')

      def child_heartbeat
        @wpipe.write HEARTBEAT_PACKET
      rescue
        @log.error "Parent process died. Exiting worker pid=#{Process.pid}"
        Process.kill(:KILL, Process.pid)
      end

      def task_heartbeat
        @running_task.heartbeat! :message => @heartbeat_message
        @heartbeat_message = nil
      rescue TaskError
        # finished, cancel_requested, preempted, etc.
        @cp.call_user_method { @runner.kill($!) }
      end
    end


    module TaskMonitorHook
      attr_accessor :task_monitor

      def heartbeat_message=(message)
        @heartbeat_message = message
        @task_monitor.set_heartbeat_message(self, message)
        message
      end

      attr_reader :heartbeat_message

      def finish!(*args, &block)
        @task_monitor.finish_task(self) {
          super(*args, &block)
        }
      end

      def release!(*args, &block)
        @task_monitor.finish_task(self) {
          super(*args, &block)
        }
      end

      def cancel_request!(*args, &block)
        @task_monitor.finish_task(self) {
          super(*args, &block)
        }
      end
    end

  end
end


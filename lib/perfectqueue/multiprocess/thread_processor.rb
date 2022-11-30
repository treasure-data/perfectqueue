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
  module Multiprocess

    class ThreadProcessor
      def initialize(runner, processor_id, config)
        @runner = runner
        @processor_id = processor_id

        @running_flag = BlockingFlag.new
        @finish_flag = BlockingFlag.new

        @tm = TaskMonitor.new(config, method(:child_heartbeat), method(:force_stop))

        restart(false, config)
      end

      def run
        @tm.start
        @running_flag.set_region do
          run_loop
        end
        @tm.join
      ensure
        @thread = nil
      end

      def join
        while t = @thread
          t.join
        end
      end

      def keepalive
        unless @thread
          @thread = Thread.new(&method(:run))
        end
      end

      def restart(immediate, config)
        @poll_interval = config[:poll_interval] || 1.0
        @log = config[:logger]
        @task_prefetch = config[:task_prefetch] || 0
        @config = config

        @tm.stop_task(immediate)

        @finish_flag.set_region do
          @running_flag.wait while @running_flag.set?
        end
      end

      def stop(immediate)
        @log.info immediate ? "Stopping thread immediately id=#{@processor_id}" : "Stopping thread gracefully id=#{@processor_id}"
        @tm.stop_task(immediate)
        @finish_flag.set!
      end

      def force_stop
        @log.error "Force stopping processor processor_id=#{@processor_id}"
        @tm.stop_task(true)
        @finish_flag.set!
      end

      def logrotated
        # do nothing
      end

      private
      def child_heartbeat
        # do nothing
      end

      def run_loop
        PerfectQueue.open(@config) {|queue|
          until @finish_flag.set?
            tasks = queue.poll_multi(:max_acquire=>1+@task_prefetch)
            if tasks == nil || tasks.empty?
              @finish_flag.wait(@poll_interval)
            else
              begin
                last_heartbeat = Time.now.to_i
                while task = tasks.shift
                  process(task, last_heartbeat)
                end
              ensure
                # TODO do not call release! because rdb_compat backend
                #      doesn't have a mechanism to detect preemption.
                #      release! could cause a problem that multiple
                #      workers run one task concurrently.
                #tasks.each {|task|
                #  # ignoring errors doesn't cause serious problems
                #  # because it's same as failure of this server.
                #  task.release! rescue nil
                #}
              end
            end
          end
        }
      rescue
        @log.error "Unknown error #{$!.class}: #{$!}: Exiting thread id=#{@processor_id}"
        $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
      ensure
        @tm.stop
      end

      def process(task, last_heartbeat=Time.now.to_i)
        @log.info "acquired task task=#{task.key} id=#{@processor_id}: #{task.inspect}"
        begin
          r = @runner.new(task)
          @tm.set_task(task, r, last_heartbeat)
          begin
            r.run
          ensure
            @tm.task_finished(task)
          end
          @log.info "completed processing task=#{task.key} id=#{@processor_id}:"
        rescue
          @log.error "unexpectedly failed task=#{task.key} id=#{@processor_id}: #{$!.class}: #{$!}"
          $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
          raise  # force exit
        end
      end
    end

  end
end


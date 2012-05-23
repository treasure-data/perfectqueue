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

    class ThreadProcessor
      def initialize(runner, config)
        @runner = runner

        @running_flag = BlockingFlag.new
        @finish_flag = BlockingFlag.new

        @tm = TaskMonitor.new(config, method(:child_heartbeat))

        restart(false, config)
      end

      def run
        @tm.start
        @running_flag.set_region do
          until @finish_flag.set?
            run_loop
          end
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
        @config = config

        @tm.stop_task(immediate)

        @finish_flag.set_region do
          @running_flag.wait while @running_flag.set?
        end
      end

      def stop(immediate)
        @log.info immediate ? "Stopping worker thread immediately" : "Stopping worker thread gracefully"
        @tm.stop_task(immediate)
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
            task = queue.poll
            if task
              process(task)
            else
              @finish_flag.wait(@poll_interval)
            end
          end
        }
      rescue
        @log.error "Unknown error #{$!.class}: #{$!}. Exiting worker pid=#{Process.pid}"
        $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
      ensure
        @tm.stop
      end

      def process(task)
        @log.info "acquired task: #{task.inspect}"
        begin
          r = @runner.new(task)
          @tm.set_task(task, r)
          begin
            r.run
          ensure
            @tm.task_finished(task)
          end
        rescue
          @log.error "process failed: #{$!.class}: #{$!}"
          $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
          raise  # force exit
        end
      end
    end

  end
end


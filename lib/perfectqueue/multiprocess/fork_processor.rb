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

    class ForkProcessor
      def initialize(runner, processor_id, config)
        @runner = runner
        @processor_id = processor_id

        @stop = false
        @cpm = nil
        @last_fork_time = 0

        restart(false, config)
      end

      def restart(immediate, config)
        @child_heartbeat_limit = config[:child_heartbeat_limit] || 60.0
        @child_kill_interval = config[:child_kill_interval] || 2.0
        @child_graceful_kill_limit = config[:child_graceful_kill_limit] || nil
        @child_fork_frequency_limit = config[:child_fork_frequency_limit] || 5.0
        @child_heartbeat_kill_delay = config[:child_heartbeat_kill_delay] || 10
        @log = config[:logger]
        @config = config  # for child process

        if c = @cpm
          c.start_killing(immediate)
        end
      end

      def stop(immediate)
        @stop = true
        if c = @cpm
          c.start_killing(immediate)
        end
        self
      end

      def keepalive
        if @stop
          try_join
          return
        end

        if c = @cpm
          if c.killing_status != true
            # don't check status if killing status is immediate-killing
            begin
              # receive heartbeat
              keptalive = c.check_heartbeat(@child_heartbeat_limit)
              if !keptalive
                @log.error "Heartbeat broke out. Restarting child process id=#{@processor_id} pid=#{c.pid}."
                c.start_killing(true)
              end
            rescue EOFError
              @log.error "Heartbeat pipe is closed. Restarting child process id=#{@processor_id} pid=#{c.pid}."
              c.start_killing(true, @child_heartbeat_kill_delay)
            rescue
              @log.error "Unknown error: #{$!.class}: #{$!}: Restarting child process id=#{@processor_id} pid=#{c.pid}."
              $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
              c.start_killing(true, @child_heartbeat_kill_delay)
            end
          end

          try_join
        end

        unless @cpm
          begin
            @cpm = fork_child
          rescue
            @log.error "Failed to fork child process id=#{@processor_id}: #{$!.class}: #{$!}"
            $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
          end
        end

        nil
      end

      def join
        while !try_join
          sleep (@child_kill_interval+1) / 2  # TODO
        end
        self
      end

      def logrotated
        if c = @cpm
          c.send_signal(:CONT)
        end
      end

      private
      def try_join
        unless @cpm
          return true
        end
        if @cpm.try_join(@child_kill_interval, @child_graceful_kill_limit)
          @cpm.cleanup
          @cpm = nil
          return true
        else
          return false
        end
      end

      INTER_FORK_LOCK = Mutex.new

      def fork_child
        now = Time.now.to_f
        if now - @last_fork_time < @child_fork_frequency_limit
          @log.info "Tried to fork child #{now-@last_fork_time} seconds ago < #{@child_fork_frequency_limit}. Waiting... id=#{@processor_id}"
          return nil
        end
        @last_fork_time = now

        # set process name
        @runner.before_fork if @runner.respond_to?(:before_fork)  # TODO exception handling

        INTER_FORK_LOCK.lock
        begin
          rpipe, wpipe = IO.pipe
          rpipe.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
          wpipe.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
        ensure
          INTER_FORK_LOCK.unlock
        end

        pid = fork do
          #STDIN.close
          # pass-through STDOUT
          # pass-through STDERR
          rpipe.close

          $0 = "perfectqueue:#{@runner} #{@processor_id}"

          @runner.after_fork if @runner.respond_to?(:after_fork)

          ChildProcess.run(@runner, @processor_id, @config, wpipe)

          exit! 0
        end

        @log.info "Worker process started. pid=#{pid}"

        wpipe.close

        ChildProcessMonitor.new(@log, pid, rpipe)
      end
    end

  end
end

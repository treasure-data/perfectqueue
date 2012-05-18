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

    class ForkProcessor
      def initialize(runner, config)
        @runner = runner

        require 'fcntl'
        @stop = false
        @cpm = nil

        restart(false, config)
      end

      def restart(immediate, config)
        @child_heartbeat_limit = config[:child_heartbeat_limit] || 10.0
        @child_kill_interval = config[:child_kill_interval] || 2.0
        @child_graceful_kill_limit = config[:child_graceful_kill_limit] || nil
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
      end

      def keepalive
        if @stop
          try_join
          return
        end

        if c = @cpm
          # receive heartbeat
          begin
            keptalive = c.check_heartbeat(@child_heartbeat_limit)
            unless keptalive
              @log.error "Heartbeat broke out. Restarting child process."
              c.start_killing(false)
            end
          rescue EOFError
            @log.error "Heartbeat pipe is closed. Restarting child process."
            c.start_killing(true)
          rescue
            @log.error "Unknown error: #{$!.class}: #{$!}. Restarting child process."
            $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
            c.start_killing(false)
          end

          try_join
        end

        unless @cpm
          begin
            @cpm = fork_child
          rescue
            @log.error "Failed to fork child process: #{$!.class}: #{$!}"
            $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
          end
        end
      end

      def join
        while !try_join
          sleep (@child_kill_interval+1) / 2  # TODO
        end
      end

      def shutdown
        stop(false)
        join
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

      def ensure_fork
        unless @cpm
          @cpm = fork_child
        end
      end

      INTER_FORK_LOCK = Mutex.new

      def fork_child
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

          @runner.after_fork if @runner.respond_to?(:after_fork)

          begin
            ChildProcess.run(@runner, @config, wpipe)
          ensure
            @runner.after_child_end if @runner.respond_to?(:after_child_end)  # TODO exception handling
          end

          exit! 0
        end

        @log.info "Worker process started. pid=#{pid}"

        wpipe.close

        ChildProcessMonitor.new(@log, pid, rpipe)
      end
    end

  end
end

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

    class Processor
      def initialize(config)
        require 'fcntl'

        @stop = false
        @child = nil

        restart(config)
      end

      def restart(immediate, config)
        return unless @child
        @child_heartbeat_limit = config[:child_heartbeat_limit] || 10.0
        @child_kill_interval = config[:child_kill_interval] || 2.0
        @child_graceful_kill_limit = config[:child_graceful_kill_limit] || nil
        @log = config[:logger]
        @config = config

        @child.start_killing(immediate)
      end

      def stop(immediate)
        @stop = true
        return unless @child
        @child.start_killing(immediate)
      end

      attr_accessor :before_fork
      attr_accessor :after_fork
      attr_accessor :before_child_end
      attr_accessor :after_child_end
      attr_accessor :runner

      def keepalive
        if @child
          begin
            keptalive = @child.read_heartbeats(@child_heartbeat_limit)
            unless keptalive
              @log.error "Heartbeat broke out. Restarting child process."
              restart(false)
            end
          rescue EOFError
            @log.error "Heartbeat pipe is closed. Restarting child process."
            restart(true)
          rescue
            @log.error "Unknown error: #{$!.class}: #{$!}. Restarting child process."
            $!.backtrace.each {|x| @log.error "  #{x}" }
            restart(false)
          end

          died = @child.try_waitpid(@child_kill_interval, @child_graceful_kill_limit)
          if died
            @child.cleanup
            @child = nil
          end
        end

        if !@child && !@stop
          begin
            @child = fork_child
          rescue
            @log.error "Failed to fork child process: #{$!.class}: #{$!}"
            $!.backtrace.each {|x| @log.error "  #{x}" }
          end
        end
      end

      def join
        while @child
          died = @child.try_waitpid(@child_kill_interval, @child_graceful_kill_limit)
          if died
            @child.cleanup
            @child = nil
          else
            sleep (@child_kill_interval+1) / 2  # TODO
          end
        end
      end

      private
      INTER_FORK_LOCK = Mutex.new

      def fork_child
        @before_fork.call if @before_fork  # TODO exception handling

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

          ChildProcess.new(@config, wpipe, @runner).main(@after_fork, @before_child_end)

          @after_child_end.call if @after_child_end  # TODO exception handling

          exit! 0
        end

        @log.info "Worker started pid=#{pid}"

        wpipe.close

        Child.new(@log, pid, rpipe)
      end

      class Child
        def initialize(log, pid, rpipe)
          @log = log
          @pid = pid
          @rpipe = rpipe
          @last_heartbeat = Time.now.to_i

          @kill_start = nil
          @kill_immediate = false
          @last_send_signal = nil

          @rbuf = ''
        end

        def read_heartbeats(limit)
          @rpipe.read_nonblock(1024, @rbuf)
          @last_heartbeat = Time.now.to_i
          return true
        rescue Errno::EINTR, Errno::EAGAIN
          return Time.now.to_i - @last_heartbeat <= limit
        end

        def start_killing(immediate)
          return if @kill_start && immediate
          @kill_immediate = true if immediate
          now = Time.now.to_i
          send_signal(now, nil)
          @kill_start = now
        end

        def try_waitpid(kill_interval, graceful_kill_limit)
          return nil unless @kill_start

          begin
            if Process.waitpid(@pid, Process::WNOHANG)
              @log.info "Worker exited pid=#{@pid}"
              return true
            end
          rescue Errno::ECHILD
            # SIGCHLD is trapped in Worker#install_signal_handlers
            @log.info "Worker exited pid=#{@pid}"
            return true
          end

          # resend signal
          now = Time.now.to_i
          if @last_send_signal + kill_interval <= now
            send_signal(now, graceful_kill_limit)
          end

          return false
        end

        def cleanup
          @rpipe.close unless @rpipe.closed?
        end

        private
        def send_signal(now, graceful_kill_limit)
          begin
            if @kill_immediate || (graceful_kill_limit && @kill_start + graceful_kill_limit < now)
              @log.debug "sending SIGKILL to pid=#{@pid} for immediate stop"
              Process.kill(:KILL, @pid)
            else
              @log.debug "sending SIGUSR1 to pid=#{@pid} for graceful stop"
              Process.kill(:TERM, @pid)
            end
          rescue Errno::ESRCH, Errno::EPERM
            # TODO log?
          end
          @last_send_signal = now
        end
      end
    end

  end
end

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

    class ChildProcessMonitor
      def initialize(log, pid, rpipe)
        @log = log
        @pid = pid
        @rpipe = rpipe
        @last_heartbeat = Time.now.to_i

        @kill_start_time = nil
        @last_kill_time = nil
        @kill_immediate = false

        @rbuf = ''
      end

      def check_heartbeat(limit)
        @rpipe.read_nonblock(1024, @rbuf)
        @last_heartbeat = Time.now.to_i
        return true
      rescue Errno::EINTR, Errno::EAGAIN
        return Time.now.to_i - @last_heartbeat <= limit
      end

      def start_killing(immediate)
        if immediate && !@kill_immediate
          @kill_immediate = true  # escalation
        elsif @kill_start_time
          return
        end

        now = Time.now.to_i
        send_signal(now, nil)
        @kill_start_time = now
      end

      def try_join(kill_interval, graceful_kill_limit)
        return nil unless @kill_start_time

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
        if @last_kill_time + kill_interval <= now
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
          if @kill_immediate || (graceful_kill_limit && @kill_start_time + graceful_kill_limit < now)
            @log.debug "sending SIGKILL to pid=#{@pid} for immediate stop"
            Process.kill(:KILL, @pid)
          else
            @log.debug "sending SIGUSR1 to pid=#{@pid} for graceful stop"
            Process.kill(:TERM, @pid)
          end
        rescue Errno::ESRCH, Errno::EPERM
          # TODO log?
        end
        @last_kill_time = now
      end
    end

  end
end

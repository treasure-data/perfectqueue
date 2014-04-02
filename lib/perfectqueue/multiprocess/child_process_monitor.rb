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

    require 'stringio'

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

      attr_reader :pid

      def check_heartbeat(limit)
        @rpipe.read_nonblock(1024, @rbuf)
        @last_heartbeat = Time.now.to_i
        return true
      rescue Errno::EINTR, Errno::EAGAIN
        return Time.now.to_i - @last_heartbeat <= limit
      end

      def start_killing(immediate, delay=0)
        if immediate && !@kill_immediate
          @kill_immediate = true  # escalation
        elsif @kill_start_time
          return
        end

        now = Time.now.to_i
        if delay == 0
          kill_children(now, nil)
          @kill_start_time = now
        else
          @kill_start_time = now + delay
        end
      end

      def killing_status
        if @kill_start_time
          if @kill_immediate
            return true
          else
            return false
          end
        else
          return nil
        end
      end

      def try_join(kill_interval, graceful_kill_limit)
        return nil unless @kill_start_time

        begin
          if Process.waitpid(@pid, Process::WNOHANG)
            @log.info "Processor exited and joined pid=#{@pid}"
            return true
          end
        rescue Errno::ECHILD
          # SIGCHLD is trapped in Supervisor#install_signal_handlers
          @log.info "Processor exited pid=#{@pid}"
          return true
        end

        # resend signal
        now = Time.now.to_i
        if @last_kill_time + kill_interval <= now
          kill_children(now, graceful_kill_limit)
        end

        return false
      end

      def cleanup
        @rpipe.close unless @rpipe.closed?
      end

      def send_signal(sig)
        begin
          Process.kill(sig, @pid)
        rescue Errno::ESRCH, Errno::EPERM
          # TODO log?
        end
      end

      private
      def kill_children(now, graceful_kill_limit)
        immediate = @kill_immediate || (graceful_kill_limit && @kill_start_time + graceful_kill_limit < now)

        if immediate
          pids = collect_child_pids(get_ppid_pid_map, [@pid], @pid)
          pids.reverse_each {|pid|
            kill_process(child, true)
          }
        else
          kill_process(@pid, false)
        end

        @last_kill_time = now
      end

      def get_ppid_pid_map
        ppid_pid = {}  # {ppid => pid}
        `ps -ao pid,ppid`.each_line do |line|
          if m = /^\s*(\d+)\s+(\d+)\s*$/.match(line)
            ppid_pid[m[2].to_i] = m[1].to_i
          end
        end
        return ppid_pid
      # We can ignore errors but not necessary
      #rescue
      #  return {}
      end

      def collect_child_pids(ppid_pid, pids, parent_pid)
        if pid = ppid_pid[parent_pid]
          pids << pid
          collect_child_pids(ppid_pid, pids, pid)
        end
        pids
      end

      def kill_process(pid, immediate)
        begin
          if immediate
            @log.debug "sending SIGKILL to pid=#{pid} for immediate stop"
            Process.kill(:KILL, pid)
          else
            @log.debug "sending SIGTERM to pid=#{pid} for graceful stop"
            Process.kill(:TERM, pid)
          end
        rescue Errno::ESRCH, Errno::EPERM
          # TODO log?
        end
      end
    end

  end
end

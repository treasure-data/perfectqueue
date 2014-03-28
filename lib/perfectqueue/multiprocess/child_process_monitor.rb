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

        if delay == 0
          now = Time.now.to_i
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
        kill_now = @kill_immediate || (graceful_kill_limit && @kill_start_time + graceful_kill_limit < now)

        child_tree = list_child_tree(@pid)
        child_tree << @pid
        child_tree.each {|child|
          kill_process(child, kill_now)
        }

        @last_kill_time = now
      end

      def list_child_tree(pid)
        children = []
        Sys::ProcTable.ps {|process|
          if process.ppid == pid
            children.insert(0, process.pid)
            children = list_child_tree(process.pid) + children
          end
        }
        children
      end

      def kill_process(pid, kill_now=true)
        begin
          if kill_now
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

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

    class ChildProcess < ThreadProcessor
      def self.run(runner, processor_id, config, wpipe)
        new(runner, processor_id, config, wpipe).run
      end

      def initialize(runner, processor_id, config, wpipe)
        @wpipe = wpipe
        @wpipe.sync = true
        @request_per_child = 0
        super(runner, processor_id, config)
        @sig = install_signal_handlers
      end

      # override
      def run
        super
      end

      # override
      def stop(immediate)
        @log.info "Exiting processor id=#{@processor_id} pid=#{Process.pid}"
        super
      end

      # override
      def join
        # do nothing
      end

      # override
      def keepalive
        # do nothing
      end

      # override
      def logrotated
        @log.reopen!
      end

      # override
      def child_heartbeat
        @wpipe.write HEARTBEAT_PACKET
      rescue
        @log.error "Parent process unexpectedly died: #{$!}"
        force_stop
      end

      # override
      def force_stop
        super
        Process.kill(:KILL, Process.pid)
        exit! 137
      end

      HEARTBEAT_PACKET = [0].pack('C')

      # override
      def restart(immediate, config)
        @max_request_per_child = config[:max_request_per_child] || nil
        super
      end

      # override
      def process(task)
        super
        if @max_request_per_child
          @request_per_child += 1
          if @request_per_child > @max_request_per_child
            stop(false)
          end
        end
      end

      private
      def install_signal_handlers
        s = self
        SignalThread.new do |st|
          st.trap :TERM do
            s.stop(false)
          end
          st.trap :INT do
            s.stop(false)
          end

          st.trap :QUIT do
            s.stop(true)
          end

          st.trap :USR1 do
            s.stop(false)
          end

          st.trap :HUP do
            s.stop(true)
          end

          st.trap :CONT do
            s.stop(false)
          end

          st.trap :WINCH do
            s.stop(true)
          end

          st.trap :USR2 do
            s.logrotated
          end

          trap :CHLD, "SIG_DFL"
        end
      end
    end

  end
end


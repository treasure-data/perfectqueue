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
        @sig.shutdown
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
        @log.error "Parent process unexpectedly died. Exiting processor pid=#{Process.pid}: #{$!}"
        stop(true)
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
        SignalQueue.start do |sig|
          sig.trap :TERM do
            stop(false)
          end
          sig.trap :INT do
            stop(false)
          end

          sig.trap :QUIT do
            stop(true)
          end

          sig.trap :USR1 do
            stop(false)
          end

          sig.trap :HUP do
            stop(true)
          end

          sig.trap :CONT do
            stop(false)
          end

          sig.trap :WINCH do
            stop(true)
          end

          sig.trap :USR2 do
            logrotated
          end

          trap :CHLD, "SIG_DFL"
        end
      end
    end

  end
end


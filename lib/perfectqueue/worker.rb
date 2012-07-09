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

  class Worker
    def self.run(runner, config=nil, &block)
      new(runner, config, &block).run
    end

    def initialize(runner, config=nil, &block)
      # initial logger
      STDERR.sync = true
      @log = DaemonsLogger.new(STDERR)

      @runner = runner
      block = Proc.new { config } if config
      @config_load_proc = block
      @finished = false
    end

    def run
      @log.info "PerfectQueue #{VERSION}"

      @sig = install_signal_handlers
      begin
        @engine = Engine.new(@runner, load_config)
        begin
          @engine.run
        ensure
          @engine.shutdown(true)
        end
      ensure
        @sig.shutdown
      end
      return nil
    rescue
      @log.error "#{$!.class}: #{$!}"
      $!.backtrace.each {|x| @log.warn "\t#{x}" }
      return nil
    end

    def stop(immediate)
      @log.info immediate ? "Received immediate stop" : "Received graceful stop"
      begin
        @engine.stop(immediate) if @engine
      rescue
        @log.error "failed to stop: #{$!}"
        $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
        return false
      end
      return true
    end

    def restart(immediate)
      @log.info immediate ? "Received immediate restart" : "Received graceful restart"
      begin
        @engine.restart(immediate, load_config)
      rescue
        @log.error "failed to restart: #{$!}"
        $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
        return false
      end
      return true
    end

    def replace(immediate, command=[$0]+ARGV)
      @log.info immediate ? "Received immediate binary replace" : "Received graceful binary replace"
      begin
        @engine.replace(immediate, command)
      rescue
        @log.error "failed to replace: #{$!}"
        $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
        return false
      end
      return true
    end

    def logrotated
      @log.info "reopen a log file"
      @engine.logrotated
      @log.reopen!
      return true
    end

    private
    def load_config
      raw_config = @config_load_proc.call
      config = {}
      raw_config.each_pair {|k,v| config[k.to_sym] = v }

      old_log = @log
      log = DaemonsLogger.new(config[:log] || STDERR)
      old_log.close if old_log
      @log = log

      config[:logger] = log

      return config
    end

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
          restart(false)
        end

        sig.trap :HUP do
          restart(true)
        end

        sig.trap :CONT do
          replace(false)
        end

        sig.trap :WINCH do
          replace(true)
        end

        sig.trap :USR2 do
          logrotated
        end

        trap :CHLD, "SIG_IGN"
      end
    end
  end

end


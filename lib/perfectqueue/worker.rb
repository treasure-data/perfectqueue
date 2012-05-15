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
    def self.run(runner, &block)
      new(runner, &block).run
    end

    def initialize(runner, &block)
      @runner = runner
      @config_load_proc = block
      @finished = false
    end

    def run
      @engine = Multiprocess::Engine.new(@runner, load_config)
      begin
        @sig = install_signal_handlers
        begin
          @engine.run
        ensure
          @sig.shutdown
        end
      ensure
        @engine.close
      end
      return nil
    rescue
      @log.error "#{$!.class}: #{$!}"
      $!.backtrace.each {|x| @log.error "  #{x}" }
      return nil
    end

    def stop
      @log.info "immediate stop"
      @engine.stop(true)
      return true
    end

    def stop_graceful
      @log.info "graceful stop"
      @engine.stop(false)
      return true
    end

    def restart
      @log.info "immediate restart"
      begin
        @engine.restart(true, load_config)
      rescue
        # TODO log
        return false
      end
      return true
    end

    def restart_graceful
      @log.info "graceful restart"
      begin
        @engine.restart(false, load_config)
      rescue
        # TODO log
        return false
      end
      return true
    end

    def replace(command=[$0]+ARGV)
      @log.info "immediate binary replace"
      @engine.replace(command, true)
      return true
    end

    def replace_graceful(command=[$0]+ARGV)
      @log.info "graceful binary replace"
      @engine.replace(command, false)
      self
    end

    def log_reopen
      @log.info "reopen a log file"
      @engine.log_reopen
      @log.reopen!
      return true
    end

    private
    def load_config
      raw_config = @config_load_proc.call
      config = {}
      raw_config.each_pair {|k,v| config[k.to_sym] = v }

      log = DaemonsLogger.new(config[:log] || STDERR)
      if old_log = @log
        old_log.close
      end
      @log = log

      config[:logger] = log

      return config
    end

    def install_signal_handlers
      SignalThread.new do |sig|
        trap :TERM do
          stop_graceful
        end
        trap :INT do
          stop_graceful
        end

        trap :QUIT do
          stop
        end

        trap :USR1 do
          restart_graceful
        end

        trap :USR2 do
          restart
        end

        trap :HUP do
          replace_graceful
        end

        trap :WINCH do
          replace
        end

        trap :CONT do
          log_reopen
        end

        trap :CHLD, "SIG_IGN"
      end
    end
  end

end


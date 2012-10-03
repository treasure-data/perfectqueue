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

  class Engine
    def initialize(runner, config)
      @runner = runner

      @finish_flag = BlockingFlag.new

      processor_type = config[:processor_type] || :process
      case processor_type.to_sym
      when :process
        @processor_class = Multiprocess::ForkProcessor
      when :thread
        @processor_class = Multiprocess::ThreadProcessor
      else
        raise ConfigError, "Unknown processor_type: #{config[:processor_type].inspect}"
      end

      @processors = []
      restart(false, config)
    end

    attr_reader :processors

    def restart(immediate, config)
      return nil if @finish_flag.set?

      # TODO connection check

      @log = config[:logger] || Logger.new(STDERR)

      num_processors = config[:processors] || 1

      # scaling
      extra = num_processors - @processors.length
      if extra > 0
        extra.times do
          @processors << @processor_class.new(@runner, @processors.size+1, config)
        end
      elsif extra < 0
        -extra.times do
          c = @processors.shift
          c.stop(immediate)
          c.join
        end
        extra = 0
      end

      @processors[0..(-extra-1)].each {|c|
        c.restart(immediate, config)
      }

      @child_keepalive_interval = (config[:child_keepalive_interval] || config[:child_heartbeat_interval] || 2).to_i

      self
    end

    def run
      @processors.each {|c|
        c.keepalive
        # add wait time before starting processors to avoid
        # a spike of the number of concurrent connections.
        sleep rand  # upto 1 second, average 0.5 seoncd
      }
      until @finish_flag.set?
        @processors.each {|c| c.keepalive }
        @finish_flag.wait(@child_keepalive_interval)
      end
      join
    end

    def stop(immediate)
      @processors.each {|c| c.stop(immediate) }
      @finish_flag.set!
      self
    end

    def join
      @processors.each {|c| c.join }
      self
    end

    def shutdown(immediate)
      stop(immediate)
      join
    end

    def replace(immediate, command=[$0]+ARGV)
      return if @replaced_pid
      stop(immediate)
      @replaced_pid = Process.fork do
        exec(*command)
        exit!(127)
      end
      self
    end

    def logrotated
      @processors.each {|c| c.logrotated }
    end
  end

end


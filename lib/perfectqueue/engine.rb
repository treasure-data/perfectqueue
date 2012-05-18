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

      @processor_class = Multiprocess::ForkProcessor

      @processors = []
      restart(false, config)
    end

    def restart(immediate, config)
      return nil if @finish_flag.set?

      # TODO connection check

      @log = config[:logger] || Logger.new(STDERR)

      num_processors = config[:processors] || 1

      # scaling
      extra = num_processors - @processors.length
      if extra > 0
        extra.times do
          @processors << @processor_class.new(@runner, config)
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
      until @finish_flag.set?
        @processors.each {|c| c.keepalive }
        @finish_flag.wait(@child_keepalive_interval)
      end
      join
    end

    def stop(immediate)
      @finish_flag.set!
      @processors.each {|c| c.stop(immediate) }
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


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
      configure(config)

      @before_fork = nil
      @after_fork = nil
      @before_child_end = nil
      @after_child_end = nil

      @running_flag = BlockingFlag.new
      @finish_flag = BlockingFlag.new

      @processors = []
      restart(true, config)
    end

    def restart(immediate, config)
      return nil if @finish_flag.set?

      # TODO connection check

      @log = config[:logger] || Logger.new(STDERR)
      # TODO log_level

      num_processors = config[:processors] || 1

      extra = num_processors - @processors.length
      if extra > 0
        extra.times do
          @processors << Multiprocess::Processor.new(self, config)
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

      @child_keepalive_interval = config[:child_keepalive_interval]

      self
    end

    def run
      @running_flag.set_region do
        until @finish_flag.set?
          @processors.each {|c| c.keepalive }
          @finish_flag.wait(@child_keepalive_interval)
        end
      end
      @processors.each {|c| c.join }
    end

    def stop(immediate)
      if @finish_flag.set!
        @processors.each {|c| c.stop(immediate) }
      end
      self
    end

    def replace(command=[$0]+ARGV, immediate)
      return if @replaced_pid
      stop(immediate)
      @replaced_pid = Process.fork do
        exec(*command)
        exit!(127)
      end
      self
    end

    def join
      @thread.join
      @processors.each {|c| c.stop(false) }
      @processors.each {|c| c.join }
      self
    end

    def log_reopen
      # TODO send signal to child processes
    end
  end

end


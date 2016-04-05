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

require 'json'
require 'thread'  # Mutex, CoditionVariable
require 'zlib'
require 'stringio'
require 'sequel'
require 'logger'
require 'fcntl'

require_relative 'perfectqueue/application'
require_relative 'perfectqueue/backend'
require_relative 'perfectqueue/backend/rdb_compat'
require_relative 'perfectqueue/blocking_flag'
require_relative 'perfectqueue/client'
require_relative 'perfectqueue/daemons_logger'
require_relative 'perfectqueue/engine'
require_relative 'perfectqueue/model'
require_relative 'perfectqueue/queue'
require_relative 'perfectqueue/runner'
require_relative 'perfectqueue/task_monitor'
require_relative 'perfectqueue/task_metadata'
require_relative 'perfectqueue/task_status'
require_relative 'perfectqueue/task'
require_relative 'perfectqueue/worker'
require_relative 'perfectqueue/supervisor'
require_relative 'perfectqueue/signal_thread'
require_relative 'perfectqueue/version'
require_relative 'perfectqueue/multiprocess/thread_processor'
require_relative 'perfectqueue/multiprocess/child_process'
require_relative 'perfectqueue/multiprocess/child_process_monitor'
require_relative 'perfectqueue/multiprocess/fork_processor'
require_relative 'perfectqueue/error'

module PerfectQueue
  def self.open(config, &block)
    c = Client.new(config)
    begin
      q = Queue.new(c)
      if block
        block.call(q)
      else
        c = nil
        return q
      end
    ensure
      c.close if c
    end
  end
end


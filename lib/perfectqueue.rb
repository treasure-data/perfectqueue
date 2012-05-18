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
  require 'json'
  require 'thread'  # Mutex, CoditionVariable

  {
    :Application => 'perfectqueue/application',
    :Backend => 'perfectqueue/backend',
    :BackendHelper => 'perfectqueue/backend',
    :BlockingFlag => 'perfectqueue/blocking_flag',
    :Client => 'perfectqueue/client',
    :DaemonsLogger => 'perfectqueue/daemons_logger',
    :Engine => 'perfectqueue/engine',
    :Model => 'perfectqueue/model',
    :Queue => 'perfectqueue/queue',
    :Runner => 'perfectqueue/runner',
    :Task => 'perfectqueue/task',
    :TaskWithMetadata => 'perfectqueue/task',
    :AcquiredTask => 'perfectqueue/task',
    :TaskMetadata => 'perfectqueue/task_metadata',
    :TaskMonitor => 'perfectqueue/task_monitor',
    :TaskMetadataAccessors => 'perfectqueue/task_metadata',
    :TaskStatus => 'perfectqueue/task_status',
    :Worker => 'perfectqueue/worker',
    :SignalQueue => 'perfectqueue/signal_queue',
  }.each_pair {|k,v|
    autoload k, File.expand_path(v, File.dirname(__FILE__))
  }
  [
    'perfectqueue/multiprocess',
    'perfectqueue/error',
  ].each {|v|
    require File.expand_path(v, File.dirname(__FILE__))
  }

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


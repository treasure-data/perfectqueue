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
    {
      :ChildProcess => 'multiprocess/child_process',
      :ChildProcessMonitor => 'multiprocess/child_process_monitor',
      :ForkProcessor => 'multiprocess/fork_processor',
      :ThreadProcessor => 'multiprocess/thread_processor',
    }.each_pair {|k,v|
      autoload k, File.expand_path(v, File.dirname(__FILE__))
    }
  end
end


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
  class Queue
    include Model

    def initialize(client)
      super(client)
    end

    def [](key)
      Task.new(@client, key)
    end

    def each(options={}, &block)
      @client.list(options, &block)
    end

    include Enumerable

    def poll(options={})
      options = options.merge({:max_acquire=>1})
      if acquired = poll_multi(options)
        return acquired[0]
      end
      return nil
    end

    def poll_multi(options={})
      @client.acquire(options)
    end

    def submit(key, type, data, options={})
      @client.submit(key, type, data, options)
    end

    def close
      client.close
    end
  end
end


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
  class Task
    include Model

    def initialize(client, key)
      super(client)
      @key = key
    end

    attr_reader :key

    def cancel_request!(options={})
      @client.cancel_request(@key, options)
    end

    def force_finish!(options={})
      @client.force_finish(@key, options)
    end

    def metadata(options={})
      @client.get_task_metadata(@key, options)
    end

    def exists?(options={})
      metadata(options)
      true
    rescue NotFoundError
      false
    end

    def preempt(options={})
      @client.preempt(@key, options)
    end

    def inspect
      "#<#{self.class} @key=#{@key.inspect}>"
    end
  end

  class TaskWithMetadata < Task
    def initialize(client, key, attributes)
      super(client, key)
      @attributes = attributes
    end

    def inspect
      "#<#{self.class} @key=#{@key.inspect} @attributes=#{@attributes.inspect}>"
    end

    include TaskMetadataAccessors
  end

  class AcquiredTask < TaskWithMetadata
    def initialize(client, key, attributes, task_token)
      super(client, key, attributes)
      @task_token = task_token
    end

    def heartbeat!(options={})
      @client.heartbeat(@task_token, options)
    end

    def finish!(options={})
      @client.finish(@task_token, options)
    end

    def release!(options={})
      @client.release(@task_token, options)
    end

    def retry!(options={})
      @client.retry(@task_token, options)
    end

    #def to_json
    #  [@key, @task_token, @attributes].to_json
    #end

    #def self.from_json(data, client)
    #  key, task_token, attributes = JSON.load(data)
    #  new(client, key, attributes, task_token)
    #end
  end
end


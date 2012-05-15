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

    def initialize(client, task_id)
      super(client)
      @task_id = task_id
    end

    attr_reader :task_id

    def cancel_request!(options={})
      @client.cancel_request(@task_id, options)
    end

    def force_finish!(options={})
      @client.force_finish(@task_id, options)
    end

    def metadata(options={})
      @client.get_task_metadata(@task_id, options)
    end

    def exists?(options={})
      metadata(options)
      true
    rescue NotFoundError
      false
    end

    def preempt(options={})
      @client.preempt(@task_id, options)
    end

    def inspect
      "#<#{self.class} @task_id=#{@task_id.inspect}>"
    end
  end

  class TaskWithMetadata < Task
    def initialize(client, task_id, attributes)
      super(client, task_id)
      @attributes = attributes
    end

    def inspect
      "#<#{self.class} @task_id=#{@task_id.inspect} @attributes=#{@attributes.inspect}>"
    end

    include TaskMetadataAccessors
  end

  class AcquiredTask < TaskWithMetadata
    def initialize(client, task_id, attributes, task_token)
      super(client, task_id, attributes)
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
    #  [@task_id, @task_token, @attributes].to_json
    #end

    #def self.from_json(data, client)
    #  task_id, task_token, attributes = JSON.load(data)
    #  new(client, task_id, attributes, task_token)
    #end
  end
end


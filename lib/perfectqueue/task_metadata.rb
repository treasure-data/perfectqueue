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
  module TaskMetadataAccessors
    attr_reader :attributes

    def type
      @attributes[:type]
    end

    def data
      @attributes[:data]
    end

    def status
      @attributes[:status]
    end

    def message
      @attributes[:message]
    end

    # TODO
    #def created_at
    #end

    def finished?
      status == TaskStatus::FINISHED
    end

    def running?
      status == TaskStatus::RUNNING
    end

    def waiting?
      status == TaskStatus::WAITING
    end

    def running?
      status == TaskStatus::RUNNING
    end

    def cancel_requested?
      status == TaskStatus::CANCEL_REQUESTED
    end
  end

  class TaskMetadata
    def initialize(client, task_id, attributes)
      super(client)
      @task_id = task_id
      @attributes = attributes
    end

    def task
      Task.new(@client, @task_id)
    end

    def inspect
      "#<#{self.class} @task_id=#{@task_id.inspect} @attributes=#{@attributes.inspect}>"
    end

    include TaskMetadataAccessors
  end
end


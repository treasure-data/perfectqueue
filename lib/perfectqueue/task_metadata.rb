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

module PerfectQueue
  module TaskMetadataAccessors
    attr_reader :attributes
    attr_reader :compression

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

    def user
      @attributes[:user]
    end

    def created_at
      if t = @attributes[:created_at]
        return Time.at(t)
      else
        return nil
      end
    end

    def timeout
      if t = @attributes[:timeout]
        return Time.at(t)
      else
        return nil
      end
    end

    def finished?
      status == TaskStatus::FINISHED
    end

    def waiting?
      status == TaskStatus::WAITING
    end

    def running?
      status == TaskStatus::RUNNING
    end
  end

  class TaskMetadata
    include Model

    def initialize(client, key, attributes)
      super(client)
      @key = key
      @compression = attributes.delete(:compression)
      @attributes = attributes
    end

    def task
      Task.new(@client, @key)
    end

    def inspect
      "#<#{self.class} @key=#{@key.inspect} @attributes=#{@attributes.inspect}>"
    end

    include TaskMetadataAccessors
  end
end


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
  class Client
    def initialize(config)
      @config = {}
      config.each_pair {|k,v| @config[k.to_sym] = v }

      @backend = Backend.new_backend(self, @config)

      @max_acquire = @config[:max_acquire] || 1
      @retention_time = @config[:retention_time] || 300
      @alive_time = @config[:alive_time] || 3600
      @retry_wait = @config[:retry_wait] || 300  # TODO retry wait algorithm
    end

    attr_reader :backend
    attr_reader :config

    def init_database(options={})
      @backend.init_database(options)
    end

    def get_task_metadata(task_id, options={})
      @backend.get_task_metadata(task_id, options)
    end

    # :message => nil
    # :alive_time => @alive_time
    def preempt(task_id, options={})
      alive_time = options[:alive_time] || @alive_time

      @backend.preempt(task_id, alive_time, options)
    end

    def list(options={}, &block)
      @backend.list(options, &block)
    end

    # :run_at => Time.now
    # :message => nil
    # :user => nil
    # :priority => nil
    def submit(task_id, type, data, options={})
      @backend.submit(task_id, type, data, options)
    end

    # :max_acquire => nil
    # :alive_time => nil
    def acquire(options={})
      alive_time = options[:alive_time] || @alive_time
      max_acquire = options[:max_acquire] || @max_acquire

      @backend.acquire(alive_time, max_acquire, options)
    end

    # :message => nil
    def cancel_request(task_id, options={})
      @backend.cancel_request(task_id, options)
    end

    def force_finish(task_id, options={})
      retention_time = options[:retention_time] || @retention_time

      @backend.force_finish(task_id, retention_time, options)
    end

    # :message => nil
    # :retention_time => default_retention_time
    def finish(task_token, options={})
      retention_time = options[:retention_time] || @retention_time

      @backend.finish(task_token, retention_time, options)
    end

    # :message => nil
    # :alive_time => nil
    def heartbeat(task_token, options={})
      alive_time = options[:alive_time] || @alive_time

      @backend.heartbeat(task_token, alive_time, options)
    end

    def release(task_token, options={})
      alive_time = options[:alive_time] || 0

      @backend.heartbeat(task_token, alive_time, options)
    end

    def retry(task_token, options={})
      alive_time = options[:retry_wait] || @retry_wait

      @backend.heartbeat(task_token, alive_time, options)
    end

    def close
      @backend.close
    end
  end
end


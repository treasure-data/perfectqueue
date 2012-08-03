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
  module Backend
    class RDBCompatBackend
      include BackendHelper

      class Token < Struct.new(:key)
      end

      def initialize(client, config)
        super

        require 'sequel'
        url = config[:url]
        @table = config[:table]
        unless @table
          raise ConfigError, ":table option is required"
        end

        #password = config[:password]
        #user = config[:user]
        @db = Sequel.connect(url, :max_connections=>1)
        @mutex = Mutex.new

        connect {
          # connection test
        }

    @sql = <<SQL
SELECT id, timeout, data, created_at, resource, max_running/running AS weight
FROM `#{@table}`
LEFT JOIN (
  SELECT resource AS res, COUNT(1) AS running
  FROM `#{@table}` AS T
  WHERE timeout > ? AND created_at IS NOT NULL AND resource IS NOT NULL
  GROUP BY resource
) AS R ON resource = res
WHERE timeout <= ? AND (max_running-running IS NULL OR max_running-running > 0)
ORDER BY weight IS NOT NULL, weight DESC, timeout ASC
LIMIT #{MAX_SELECT_ROW}
SQL

        # sqlite doesn't support SELECT ... FOR UPDATE but
        # sqlite doesn't need it because the db is not shared
        unless url.split('//',2)[0].to_s.include?('sqlite')
          @sql << 'FOR UPDATE'
        end
      end

      attr_reader :db

      MAX_SELECT_ROW = 8
      MAX_RESOURCE = (ENV['PQ_MAX_RESOURCE'] || 4).to_i
      #KEEPALIVE = 10
      MAX_RETRY = 10

      def init_database(options)
        sql = %[
            CREATE TABLE IF NOT EXISTS `#{@table}` (
              id VARCHAR(256) NOT NULL,
              timeout INT NOT NULL,
              data BLOB NOT NULL,
              created_at INT,
              resource VARCHAR(256),
              max_running INT,
              PRIMARY KEY (id)
            );]
        connect {
          @db.run sql
        }
      end

      # => TaskStatus
      def get_task_metadata(key, options)
        now = (options[:now] || Time.now).to_i

        connect {
          row = @db.fetch("SELECT timeout, data, created_at, resource FROM `#{@table}` WHERE id=? LIMIT 1", key).first
          unless row
            raise NotFoundError, "task key=#{key} does no exist"
          end
          attributes = create_attributes(now, row)
          return TaskMetadata.new(@client, key, attributes)
        }
      end

      # => AcquiredTask
      def preempt(key, alive_time, options)
        raise NotSupportedError.new("preempt is not supported by rdb_compat backend")
      end

      # yield [TaskWithMetadata]
      def list(options, &block)
        now = (options[:now] || Time.now).to_i

        connect {
          #@db.fetch("SELECT id, timeout, data, created_at, resource FROM `#{@table}` WHERE !(created_at IS NULL AND timeout <= ?) ORDER BY timeout ASC;", now) {|row|
          @db.fetch("SELECT id, timeout, data, created_at, resource FROM `#{@table}` ORDER BY timeout ASC;", now) {|row|
            attributes = create_attributes(now, row)
            task = TaskWithMetadata.new(@client, row[:id], attributes)
            yield task
          }
        }
      end

      # => Task
      def submit(key, type, data, options)
        now = (options[:now] || Time.now).to_i
        run_at = (options[:run_at] || now).to_i
        user = options[:user]
        user = user.to_s if user
        max_running = options[:max_running]
        data = data ? data.dup : {}
        data['type'] = type

        connect {
          begin
            n = @db[
              "INSERT INTO `#{@table}` (id, timeout, data, created_at, resource, max_running) VALUES (?, ?, ?, ?, ?, ?);",
              key, run_at, data.to_json, now, user, max_running
            ].insert
            return Task.new(@client, key)
          rescue Sequel::DatabaseError
            raise IdempotentAlreadyExistsError, "task key=#{key} already exists"
          end
        }
      end

      # => [AcquiredTask]
      def acquire(alive_time, max_acquire, options)
        now = (options[:now] || Time.now).to_i
        next_timeout = now + alive_time

        connect {
          while true
            rows = 0
            @db.transaction do
              @db.fetch(@sql, now, now) {|row|
                unless row[:created_at]
                  # finished task
                  @db["DELETE FROM `#{@table}` WHERE id=?;", row[:id]].delete

                else
                  ## optimistic lock is not needed because the row is locked for update
                  #n = @db["UPDATE `#{@table}` SET timeout=? WHERE id=? AND timeout=?", timeout, row[:id], row[:timeout]].update
                  n = @db["UPDATE `#{@table}` SET timeout=? WHERE id=?", next_timeout, row[:id]].update
                  if n > 0
                    attributes = create_attributes(nil, row)
                    task_token = Token.new(row[:id])
                    task = AcquiredTask.new(@client, row[:id], attributes, task_token)
                    return [task]
                  end
                end

                rows += 1
              }
            end
            break nil if rows < MAX_SELECT_ROW
          end
        }
      end

      # => nil
      def cancel_request(key, options)
        now = (options[:now] || Time.now).to_i

        # created_at=-1 means cancel_requested
        connect {
          n = @db["UPDATE `#{@table}` SET created_at=-1 WHERE id=? AND created_at IS NOT NULL;", key].update
          if n <= 0
            raise AlreadyFinishedError, "task key=#{key} does not exist or already finished."
          end
        }
        nil
      end

      def force_finish(key, retention_time, options)
        finish(Token.new(key), retention_time, options)
      end

      # => nil
      def finish(task_token, retention_time, options)
        now = (options[:now] || Time.now).to_i
        delete_timeout = now + retention_time
        key = task_token.key

        connect {
          n = @db["UPDATE `#{@table}` SET timeout=?, created_at=NULL, resource=NULL WHERE id=? AND created_at IS NOT NULL;", delete_timeout, key].update
          if n <= 0
            raise IdempotentAlreadyFinishedError, "task key=#{key} does not exist or already finished."
          end
        }
        nil
      end

      # => nil
      def heartbeat(task_token, alive_time, options)
        now = (options[:now] || Time.now).to_i
        next_timeout = now + alive_time
        key = task_token.key

        connect {
          n = @db["UPDATE `#{@table}` SET timeout=? WHERE id=? AND created_at IS NOT NULL;", next_timeout, key].update
          if n <= 0
            row = @db.fetch("SELECT id, timeout, created_at FROM `#{@table}` WHERE id=? LIMIT 1", key).first
            if row == nil
              raise PreemptedError, "task key=#{key} does not exist or preempted."
            elsif row[:created_at] == -1
              raise CancelRequestedError, "task key=#{key} is cancel requested."
            elsif row[:timeout] == next_timeout
              # ok
            else
              # row[:created_at] == null
              raise PreemptedError, "task key=#{key} preempted."
            end
          end
        }
        nil
      end

      protected
      def connect(&block)
        #now = Time.now.to_i
        @mutex.synchronize do
          #if now - @last_time > KEEPALIVE
          #  @db.disconnect
          #end
          #@last_time = now
          retry_count = 0
          begin
            block.call
          rescue
            # workaround for "Mysql2::Error: Deadlock found when trying to get lock; try restarting transaction" error
            if $!.to_s.include?('try restarting transaction')
              err = ([$!] + $!.backtrace.map {|bt| "  #{bt}" }).join("\n")
              retry_count += 1
              if retry_count < MAX_RETRY
                STDERR.puts err + "\n  retrying."
                sleep 0.5
                retry
              else
                STDERR.puts err + "\n  abort."
              end
            end
            raise
          ensure
            @db.disconnect
          end
        end
      end

      def create_attributes(now, row)
        if row[:created_at] === nil
          created_at = nil  # unknown creation time
          status = TaskStatus::FINISHED
        elsif row[:created_at] == -1
          created_at = 0
          status = TaskStatus::CANCEL_REQUESTED
        elsif now && row[:timeout] < now
          created_at = row[:created_at]
          status = TaskStatus::WAITING
        else
          created_at = row[:created_at]
          status = TaskStatus::RUNNING
        end

        d = row[:data]
        if d == nil || d == ''
          data = {}
        else
          begin
            data = JSON.parse(d)
          rescue
            data = {}
          end
        end

        type = data.delete('type')
        if type == nil || type.empty?
          type = row[:id].split(/\./, 2)[0]
        end

        attributes = {
          :status => status,
          :created_at => created_at,
          :data => data,
          :type => type,
          :user => row[:resource],
          :timeout => row[:timeout],
          :message => nil,  # not supported
          :node => nil,  # not supported
        }
      end

    end
  end
end


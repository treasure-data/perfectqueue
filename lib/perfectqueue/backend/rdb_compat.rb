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

        case url.split('//',2)[0].to_s
        when /sqlite/i
          @db = Sequel.connect(url, :max_connections=>1)
        when /mysql/i
          require 'uri'

          uri = URI.parse(url)
          options = {
            user: uri.user,
            password: uri.password,
            host: uri.host,
            port: uri.port ? uri.port.to_i : 3306,
            max_connections: 1
          }
          options[:sslca] = config[:sslca] if config[:sslca]

          db_name = uri.path.split('/')[1]
          @db = Sequel.mysql2(db_name, options)
          if config[:use_connection_pooling]
            @use_connection_pooling = config[:use_connection_pooling]
          else
            @use_connection_pooling = !!config[:sslca]
          end
        else
          raise ConfigError, "'sqlite' and 'mysql' are supported"
        end

        @last_time = Time.now.to_i
        @mutex = Mutex.new

        connect {
          # connection test
        }

        if config[:disable_resource_limit]
          @sql = <<SQL
SELECT id, timeout, data, created_at, resource
FROM `#{@table}`
WHERE timeout <= ? AND timeout <= ? AND created_at IS NOT NULL
ORDER BY timeout ASC
LIMIT ?
SQL
        else
          @sql = <<SQL
SELECT id, timeout, data, created_at, resource, max_running, max_running/running AS weight
FROM `#{@table}`
LEFT JOIN (
  SELECT resource AS res, COUNT(1) AS running
  FROM `#{@table}` AS T
  WHERE timeout > ? AND created_at IS NOT NULL AND resource IS NOT NULL
  GROUP BY resource
) AS R ON resource = res
WHERE timeout <= ? AND created_at IS NOT NULL AND (max_running-running IS NULL OR max_running-running > 0)
ORDER BY weight IS NOT NULL, weight DESC, timeout ASC
LIMIT ?
SQL
        end

        case url.split('//',2)[0].to_s
        when /sqlite/i
          # sqlite always locks tables on BEGIN
          @table_lock = nil
        when /mysql/i
          if config[:disable_resource_limit]
            @table_lock = "LOCK TABLES `#{@table}` WRITE"
          else
            @table_lock = "LOCK TABLES `#{@table}` WRITE, `#{@table}` AS T WRITE"
          end
        else
          @table_lock = "LOCK TABLE `#{@table}`"
        end

        @prefetch_break_types = config[:prefetch_break_types] || []

        @cleanup_interval = config[:cleanup_interval] || DEFAULT_DELETE_INTERVAL
        @cleanup_interval_count = 0
      end

      attr_reader :db

      KEEPALIVE = 10
      MAX_RETRY = 10
      DEFAULT_DELETE_INTERVAL = 20

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
          row = @db.fetch("SELECT timeout, data, created_at, resource, max_running FROM `#{@table}` WHERE id=? LIMIT 1", key).first
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
          @db.fetch("SELECT id, timeout, data, created_at, resource, max_running FROM `#{@table}` ORDER BY timeout ASC", now) {|row|
            attributes = create_attributes(now, row)
            task = TaskWithMetadata.new(@client, row[:id], attributes)
            yield task
          }
        }
      end

      # => Task
      def submit(key, type, data, options)
        now = (options[:now] || Time.now).to_i
        now = 1 if now < 1  # 0 means cancel requested
        run_at = (options[:run_at] || now).to_i
        user = options[:user]
        user = user.to_s if user
        max_running = options[:max_running]
        data = data ? data.dup : {}
        data['type'] = type

        d = data.to_json

        if options[:compression] == 'gzip'
          require 'zlib'
          require 'stringio'
          io = StringIO.new
          gz = Zlib::GzipWriter.new(io)
          begin
            gz.write(d)
          ensure
            gz.close
          end
          d = io.string
          d.force_encoding('ASCII-8BIT') if d.respond_to?(:force_encoding)
          d = Sequel::SQL::Blob.new(d)
        end

        connect {
          begin
            n = @db[
              "INSERT INTO `#{@table}` (id, timeout, data, created_at, resource, max_running) VALUES (?, ?, ?, ?, ?, ?)",
              key, run_at, d, now, user, max_running
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

        tasks = []

        connect {
          if @cleanup_interval_count <= 0
            @db["DELETE FROM `#{@table}` WHERE timeout <= ? AND created_at IS NULL", now].delete
            @cleanup_interval_count = @cleanup_interval
          end

          @db.transaction do
            if @table_lock
              @db[@table_lock].update
            end

            tasks = []
            @db.fetch(@sql, now, now, max_acquire) {|row|
              attributes = create_attributes(nil, row)
              task_token = Token.new(row[:id])
              task = AcquiredTask.new(@client, row[:id], attributes, task_token)
              tasks.push task

              if @prefetch_break_types.include?(attributes[:type])
                break
              end
            }

            if tasks.empty?
              return nil
            end

            sql = "UPDATE `#{@table}` SET timeout=? WHERE id IN ("
            params = [sql, next_timeout]
            tasks.each {|t| params << t.key }
            sql << (1..tasks.size).map { '?' }.join(',')
            sql << ") AND created_at IS NOT NULL"

            n = @db[*params].update
            if n != tasks.size
              # TODO table lock doesn't work. error?
            end

            @cleanup_interval_count -= 1
          end
          return tasks
        }
      end

      # => nil
      def cancel_request(key, options)
        now = (options[:now] || Time.now).to_i

        # created_at=0 means cancel_requested
        connect {
          n = @db["UPDATE `#{@table}` SET created_at=0 WHERE id=? AND created_at IS NOT NULL", key].update
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
          n = @db["UPDATE `#{@table}` SET timeout=?, created_at=NULL, resource=NULL WHERE id=? AND created_at IS NOT NULL", delete_timeout, key].update
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
        data = options[:data]

        sql = "UPDATE `#{@table}` SET timeout=?"
        params = [sql, next_timeout]
        if data
          sql << ", data=?"
          params << data.to_json
        end
        sql << " WHERE id=? AND created_at IS NOT NULL"
        params << key

        connect {
          n = @db[*params].update
          if n <= 0
            row = @db.fetch("SELECT id, timeout, created_at FROM `#{@table}` WHERE id=? LIMIT 1", key).first
            if row == nil
              raise PreemptedError, "task key=#{key} does not exist or preempted."
            elsif row[:created_at] == nil
              raise PreemptedError, "task key=#{key} preempted."
            elsif row[:created_at] <= 0
              raise CancelRequestedError, "task key=#{key} is cancel requested."
            else # row[:timeout] == next_timeout
              # ok
            end
          end
        }
        nil
      end

      def release(task_token, alive_time, options)
        heartbeat(task_token, alive_time, options)
      end

      protected
      def connect(&block)
        now = Time.now.to_i
        @mutex.synchronize do
          if !@use_connection_pooling || now - @last_time > KEEPALIVE
            @db.disconnect
          end

          count = 0
          begin
            block.call
            @last_time = now
          rescue
            # workaround for "Mysql2::Error: Deadlock found when trying to get lock; try restarting transaction" error
            if $!.to_s.include?('try restarting transaction')
              err = ([$!] + $!.backtrace.map {|bt| "  #{bt}" }).join("\n")
              count += 1
              if count < MAX_RETRY
                STDERR.puts err + "\n  retrying."
                sleep rand
                retry
              else
                STDERR.puts err + "\n  abort."
              end
            end

            STDOUT.puts "disconnects current connection: #{err}"
            @db.disconnect

            raise
          end
        end
      end

      GZIP_MAGIC_BYTES = [0x1f, 0x8b].pack('CC')

      def create_attributes(now, row)
        if row[:created_at] === nil
          created_at = nil  # unknown creation time
          status = TaskStatus::FINISHED
        elsif row[:created_at] <= 0
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
          # automatic gzip decompression
          d.force_encoding('ASCII-8BIT') if d.respond_to?(:force_encoding)
          if d[0, 2] == GZIP_MAGIC_BYTES
            require 'zlib'
            require 'stringio'
            gz = Zlib::GzipReader.new(StringIO.new(d))
            begin
              d = gz.read
            ensure
              gz.close
            end
          end

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
          :max_running => row[:max_running],
          :message => nil,  # not supported
          :node => nil,  # not supported
        }
      end

    end
  end
end


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

      #
      # == timeout model
      #
      # 0 ---- now-1Bs ---- retention ---|----- now -- alive ------- FUTURE
      #   ~~~~~~~^  to be deleted ^      |~~~^~~~       ^ running or in-queue
      #    DELETE          13_0000_0000->|   to be acquired
      #
      # NOTE: this architecture introduces Year 2042 problem.
      #
      DELETE_OFFSET = 10_0000_0000
      EVENT_HORIZON = 13_0000_0000 # 2011-03-13 07:06:40 UTC

      class Token < Struct.new(:key)
      end

      def initialize(client, config)
        super

        url = config[:url]
        @table = config[:table]
        unless @table
          raise ConfigError, ":table option is required"
        end

        if /\Amysql2:/i =~ url
          @db = Sequel.connect(url, {max_connections: 1, sslca: config[:sslca]})
          if config.fetch(:use_connection_pooling, nil) != nil
            @use_connection_pooling = !!config[:use_connection_pooling]
          else
            @use_connection_pooling = !!config[:sslca]
          end
          @table_lock = lambda {
            locked = nil
            loop do
              @db.fetch("SELECT GET_LOCK('#{@table}', #{LOCK_WAIT_TIMEOUT}) locked") do |row|
                locked = true if row[:locked] == 1
              end
              break if locked
            end
          }
          @table_unlock = lambda {
            @db.run("DO RELEASE_LOCK('#{@table}')")
          }
        else
          raise ConfigError, "only 'mysql' is supported"
        end

        @last_time = Time.now.to_i
        @mutex = Mutex.new

        connect {
          # connection test
        }

        # MySQL's CONNECTION_ID() is a 64bit unsigned integer from the
        # server's internal thread ID counter. It is unique while the MySQL
        # server is running.
        # https://bugs.mysql.com/bug.php?id=19806
        #
        # An acquired task is marked with next_timeout and CONNECTION_ID().
        # Therefore while alive_time is not changed and we don't restart
        # the server in 1 second, they won't conflict.
        if config[:disable_resource_limit]
          @update_sql = <<SQL
UPDATE `#{@table}`
  JOIN (
  SELECT id
    FROM `#{@table}` FORCE INDEX (`index_#{@table}_on_timeout`)
   WHERE #{EVENT_HORIZON} < timeout AND timeout <= :now
   ORDER BY timeout ASC
      LIMIT :max_acquire FOR UPDATE) AS t1 USING(id)
   SET timeout=:next_timeout, owner=CONNECTION_ID()
SQL
          @sql = <<SQL
SELECT id, timeout, data, created_at, resource
  FROM `#{@table}`
 WHERE timeout = ? AND owner = CONNECTION_ID()
SQL
        else
          @update_sql = <<SQL
UPDATE `#{@table}`
  JOIN (
    SELECT id, IFNULL(max_running, 1) / (IFNULL(running, 0) + 1) AS weight
    FROM `#{@table}`
    LEFT JOIN (
      SELECT resource, COUNT(1) AS running
      FROM `#{@table}` AS t1
      WHERE timeout > :now AND resource IS NOT NULL
      GROUP BY resource
      FOR UPDATE
    ) AS t2 USING(resource)
    WHERE #{EVENT_HORIZON} < timeout AND timeout <= :now AND IFNULL(max_running - running, 1) > 0
    ORDER BY weight DESC, timeout ASC
    LIMIT :max_acquire
    FOR UPDATE
  ) AS t3 USING (id)
SET timeout = :next_timeout, owner = CONNECTION_ID()
SQL
          @sql = <<SQL
SELECT id, timeout, data, created_at, resource, max_running
  FROM `#{@table}`
 WHERE timeout = ? AND owner = CONNECTION_ID()
SQL
        end

        @cleanup_interval = config[:cleanup_interval] || DEFAULT_DELETE_INTERVAL
        # If cleanup_interval > max_request_per_child / max_acquire,
        # some processes won't run DELETE query.
        # (it's not an issue when there are enough workers)
        @cleanup_interval_count = @cleanup_interval > 0 ? rand(@cleanup_interval) : 0
      end

      attr_reader :db

      KEEPALIVE = 10
      MAX_RETRY = 10
      LOCK_WAIT_TIMEOUT = 60
      DEFAULT_DELETE_INTERVAL = 20

      def init_database(options)
        sql = []
        sql << "DROP TABLE IF EXISTS `#{@table}`" if options[:force]
        sql << <<-SQL
          CREATE TABLE IF NOT EXISTS `#{@table}` (
            id VARCHAR(255) NOT NULL,
            timeout INT NOT NULL,
            data LONGBLOB NOT NULL,
            created_at INT,
            resource VARCHAR(255),
            max_running INT,
            /* CONNECTION_ID() can be 64bit: https://bugs.mysql.com/bug.php?id=19806 */
            owner BIGINT(21) UNSIGNED NOT NULL DEFAULT 0,
            PRIMARY KEY (id)
          )
          SQL
        sql << "CREATE INDEX `index_#{@table}_on_timeout` ON `#{@table}` (`timeout`)"
        connect {
          sql.each(&@db.method(:run))
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
          @db.fetch("SELECT id, timeout, data, created_at, resource, max_running FROM `#{@table}` ORDER BY timeout ASC") {|row|
            attributes = create_attributes(now, row)
            task = TaskWithMetadata.new(@client, row[:id], attributes)
            yield task
          }
        }
      end

      def compress_data(data, compression)
        if compression == 'gzip'
          io = StringIO.new
          io.set_encoding(Encoding::ASCII_8BIT)
          gz = Zlib::GzipWriter.new(io)
          begin
            gz.write(data)
          ensure
            gz.close
          end
          data = io.string
          data = Sequel::SQL::Blob.new(data)
        end
        data
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
        d = compress_data(data.to_json, options[:compression])

        connect {
          begin
            @db[
              "INSERT INTO `#{@table}` (id, timeout, data, created_at, resource, max_running) VALUES (?, ?, ?, ?, ?, ?)",
              key, run_at, d, now, user, max_running
            ].insert
            return Task.new(@client, key)
          rescue Sequel::UniqueConstraintViolation
            raise IdempotentAlreadyExistsError, "task key=#{key} already exists"
          end
        }
      end

      # => [AcquiredTask]
      def acquire(alive_time, max_acquire, options)
        now = (options[:now] || Time.now).to_i
        next_timeout = now + alive_time
        tasks = nil
        t0 = nil

        if @cleanup_interval_count <= 0
          connect {
            t0=Process.clock_gettime(Process::CLOCK_MONOTONIC)
            @db["DELETE FROM `#{@table}` WHERE timeout <= ?", now-DELETE_OFFSET].delete
            @cleanup_interval_count = @cleanup_interval
            STDERR.puts"PQ:delete from #{@table}:%6f sec" % [Process.clock_gettime(Process::CLOCK_MONOTONIC)-t0]
          }
        end

        connect_locked {
          t0=Process.clock_gettime(Process::CLOCK_MONOTONIC)
          n = @db[@update_sql, next_timeout: next_timeout, now: now, max_acquire: max_acquire].update
          if n <= 0
            return nil
          end

          tasks = []
          @db.fetch(@sql, next_timeout) {|row|
            attributes = create_attributes(nil, row)
            task_token = Token.new(row[:id])
            task = AcquiredTask.new(@client, row[:id], attributes, task_token)
            tasks.push task
          }
          @cleanup_interval_count -= 1

          return tasks
        }
      ensure
        STDERR.puts "PQ:acquire from #{@table}:%6f sec (%d tasks)" % [Process.clock_gettime(Process::CLOCK_MONOTONIC)-t0,tasks.size] if tasks
      end

      def force_finish(key, retention_time, options)
        finish(Token.new(key), retention_time, options)
      end

      # => nil
      def finish(task_token, retention_time, options)
        now = (options[:now] || Time.now).to_i
        delete_timeout = now - DELETE_OFFSET + retention_time
        key = task_token.key

        connect {
          n = @db["UPDATE `#{@table}` SET timeout=?, created_at=NULL, resource=NULL WHERE id=? AND #{EVENT_HORIZON} < timeout", delete_timeout, key].update
          if n <= 0
            raise IdempotentAlreadyFinishedError, "task key=#{key} does not exist or already finished."
          end
        }
        nil
      end

      # => next_timeout
      def heartbeat(task_token, alive_time, options)
        now = (options[:now] || Time.now).to_i
        next_timeout = now + alive_time
        key = task_token.key
        data = options[:data]

        sql = "UPDATE `#{@table}` SET timeout=?"
        params = [sql, next_timeout]
        if data
          sql << ", data=?"
          params << compress_data(data.to_json, options[:compression])
        end
        if last_heartbeat = options[:last_heartbeat]
          sql << " WHERE id=? AND timeout=?"
          params << key
          params << last_heartbeat
        else
          sql << " WHERE id=? AND #{EVENT_HORIZON} < timeout"
          params << key
        end

        connect {
          n = @db[*params].update
          if n <= 0
            row = @db.fetch("SELECT id, timeout, created_at FROM `#{@table}` WHERE id=? LIMIT 1", key).first
            if row == nil
              raise PreemptedError, "task key=#{key} does not exist or preempted."
            elsif row[:created_at] == nil
              raise PreemptedError, "task key=#{key} is finished or canceled"
            elsif options[:last_heartbeat] && row[:timeout] != options[:last_heartbeat]
              raise PreemptedError, "task key=#{key} is preempted by another worker."
            else # row[:timeout] == next_timeout
              # ok
            end
          end
        }
        next_timeout
      end

      def release(task_token, alive_time, options)
        heartbeat(task_token, alive_time, options)
      end

      protected
      def connect_locked(&block)
        connect {
          locked = false

          begin
            if @table_lock
              @table_lock.call
              locked = true
            end

            return block.call
          ensure
            if @use_connection_pooling && locked
              @table_unlock.call
            end
          end
        }
      end

      def connect(&block)
        now = Time.now.to_i
        @mutex.synchronize do
          # keepalive_timeout
          @db.disconnect if now - @last_time > KEEPALIVE

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
            else
              err = $!
            end

            STDERR.puts "disconnects current connection: #{err}"
            @db.disconnect

            raise
          ensure
            # connection_pooling
            @db.disconnect if !@use_connection_pooling
          end
        end
      end

      GZIP_MAGIC_BYTES = [0x1f, 0x8b].pack('CC')

      def create_attributes(now, row)
        compression = nil
        if row[:created_at] === nil
          created_at = nil  # unknown creation time
          status = TaskStatus::FINISHED
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
            compression = 'gzip'
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

        {
          :status => status,
          :created_at => created_at,
          :data => data,
          :type => type,
          :user => row[:resource],
          :timeout => row[:timeout],
          :max_running => row[:max_running],
          :message => nil,  # not supported
          :node => nil,  # not supported
          :compression => compression,
        }
      end

    end
  end
end


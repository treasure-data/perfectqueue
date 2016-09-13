require 'sequel'
require 'uri'
require_relative 'rdb_compat'

module PerfectQueue::Backend
  class RDBBackend
    MAX_RETRY = ::PerfectQueue::Backend::RDBCompatBackend::MAX_RETRY
    DELETE_OFFSET = ::PerfectQueue::Backend::RDBCompatBackend::DELETE_OFFSET
    EVENT_HORIZON = ::PerfectQueue::Backend::RDBCompatBackend::EVENT_HORIZON
    class Token < Struct.new(:key)
    end

    def initialize(uri, table, config={})
      @uri = uri
      @table = table

      u = URI.parse(@uri)
      options = {
        max_connections: 1,
        user: u.user,
        password: u.password,
        host: u.host,
        port: u.port ? u.port.to_i : 3306
      }
      options[:sslca] = config[:sslca] if config[:sslca]
      db_name = u.path.split('/')[1]
      @db = Sequel.mysql2(db_name, options)

      @mutex = Mutex.new
      connect {
        # connection test
      }
    end

    attr_reader :db

    def submit(id, data, time=Process.clock_gettime(Process::CLOCK_REALTIME, :second), resource=nil, max_running=nil)
      connect {
        begin
          data = Sequel::SQL::Blob.new(data)
          @db.sql_log_level = :debug
          n = @db["INSERT INTO `#{@table}` (id, timeout, data, created_at, resource, max_running) VALUES (?, ?, ?, ?, ?, ?);", id, time, data, time, resource, max_running].insert
          return true
        rescue Sequel::UniqueConstraintViolation => e
          return nil
        end
      }
    end

    def cancel(id, delete_timeout=3600, now=Process.clock_gettime(Process::CLOCK_REALTIME, :second))
      connect {
        n = @db["UPDATE `#{@table}` SET timeout=?, created_at=NULL, resource=NULL WHERE id=? AND #{EVENT_HORIZON} < timeout;", now+delete_timeout-DELETE_OFFSET, id].update
        return n > 0
      }
    end

    private
    def connect(&block)
      @mutex.synchronize do
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
  end
end

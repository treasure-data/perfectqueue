
module PerfectQueue


class RDBBackend < Backend
  def initialize(uri, table)
    require 'sequel'
    @uri = uri
    @table = table
    @db = Sequel.connect(@uri, :max_connections=>1)
    #@last_time = Time.now.to_i
    @mutex = Mutex.new
    #init_db(@uri.split('//',2)[0])
    connect {
      # connection test
    }
    @sql = <<SQL
SELECT id, timeout, data, created_at, resource
FROM `#{@table}`
LEFT JOIN (
  SELECT resource AS res, COUNT(1) AS running
  FROM `#{@table}` AS T
  WHERE timeout > ? AND created_at IS NOT NULL AND resource IS NOT NULL
  GROUP BY resource
) AS R ON resource = res
WHERE timeout <= ? AND (running IS NULL OR running < #{MAX_RESOURCE})
ORDER BY timeout ASC LIMIT #{MAX_SELECT_ROW}
SQL
    # sqlite doesn't support SELECT ... FOR UPDATE but
    # sqlite doesn't need it because the db is not shared
    unless @uri.split('//',2)[0].to_s.include?('sqlite')
      @sql << 'FOR UPDATE'
    end
  end

  def create_tables
    sql = ''
    sql << "CREATE TABLE IF NOT EXISTS `#{@table}` ("
    sql << "  id VARCHAR(256) NOT NULL,"
    sql << "  timeout INT NOT NULL,"
    sql << "  data BLOB NOT NULL,"
    sql << "  created_at INT,"
    sql << "  resource VARCHAR(256),"
    sql << "  PRIMARY KEY (id)"
    sql << ");"
    # TODO index
    connect {
      @db.run sql
    }
  end

  private
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

  public
  def list(&block)
    @db.fetch("SELECT id, timeout, data, created_at, resource FROM `#{@table}` WHERE created_at IS NOT NULL ORDER BY timeout ASC;") {|row|
      yield row[:id], row[:created_at], row[:data], row[:timeout], row[:resource]
    }
  end

  MAX_SELECT_ROW = 8
  MAX_RESOURCE = (ENV['PQ_MAX_RESOURCE'] || 4).to_i
  #KEEPALIVE = 10
  MAX_RETRY = 10

  def acquire(timeout, now=Time.now.to_i)
    connect {
      while true
        rows = 0
        @db.transaction do
          @db.fetch(@sql, now, now) {|row|
            unless row[:created_at]
              # finished/canceled task
              @db["DELETE FROM `#{@table}` WHERE id=?;", row[:id]].delete

            else
              ## optimistic lock is not needed because the row is locked for update
              #n = @db["UPDATE `#{@table}` SET timeout=? WHERE id=? AND timeout=?", timeout, row[:id], row[:timeout]].update
              n = @db["UPDATE `#{@table}` SET timeout=? WHERE id=?", timeout, row[:id]].update
              if n > 0
                return row[:id], Task.new(row[:id], row[:created_at], row[:data], row[:resource])
              end
            end

            rows += 1
          }
        end
        break nil if rows < MAX_SELECT_ROW
      end
    }
  end

  def finish(id, delete_timeout=3600, now=Time.now.to_i)
    connect {
      n = @db["UPDATE `#{@table}` SET timeout=?, created_at=NULL, resource=NULL WHERE id=? AND created_at IS NOT NULL;", now+delete_timeout, id].update
      return n > 0
    }
  end

  def update(id, timeout)
    connect {
      n = @db["UPDATE `#{@table}` SET timeout=? WHERE id=? AND created_at IS NOT NULL;", timeout, id].update
      if n <= 0
        raise CanceledError, "Task id=#{id} is canceled."
      end
      return nil
    }
  end

  def cancel(id, delete_timeout=3600, now=Time.now.to_i)
    finish(id, delete_timeout, now)
  end

  def submit(id, data, time=Time.now.to_i, resource=nil)
    connect {
      begin
        n = @db["INSERT INTO `#{@table}` (id, timeout, data, created_at, resource) VALUES (?, ?, ?, ?, ?);", id, time, data, time, resource].insert
        return true
      rescue Sequel::DatabaseError
        return nil
      end
    }
  end
end


end


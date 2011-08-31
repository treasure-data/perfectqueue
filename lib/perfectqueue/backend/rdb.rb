
module PerfectQueue


class RDBBackend < Backend
  def initialize(uri, table)
    require 'sequel'
    @uri = uri
    @table = table
    @db = Sequel.connect(@uri)
    init_db(@uri.split(':',2)[0])
  end

  private
  def init_db(type)
    sql = ''
    case type
    when /mysql/i
      sql << "CREATE TABLE IF NOT EXISTS `#{@table}` ("
      sql << "  id VARCHAR(256) NOT NULL,"
      sql << "  timeout INT NOT NULL,"
      sql << "  data BLOB NOT NULL,"
      sql << "  created_at INT,"
      sql << "  PRIMARY KEY (id)"
      sql << ") ENGINE=INNODB;"
    else
      sql << "CREATE TABLE IF NOT EXISTS `#{@table}` ("
      sql << "  id VARCHAR(256) NOT NULL,"
      sql << "  timeout INT NOT NULL,"
      sql << "  data BLOB NOT NULL,"
      sql << "  created_at INT,"
      sql << "  PRIMARY KEY (id)"
      sql << ");"
    end
    # TODO index
    connect {
      @db.run sql
    }
  end

  def connect(&block)
    begin
      block.call
    ensure
      @db.disconnect
    end
  end

  public
  def list(&block)
    @db.fetch("SELECT id, timeout, data, created_at FROM `#{@table}` WHERE created_at IS NOT NULL ORDER BY timeout ASC;") {|row|
      yield row[:id], row[:created_at], row[:data], row[:timeout]
    }
  end

  MAX_SELECT_ROW = 32

  def acquire(timeout, now=Time.now.to_i)
    connect {
      while true
        rows = 0
        @db.fetch("SELECT id, timeout, data, created_at FROM `#{@table}` WHERE timeout <= ? ORDER BY timeout ASC LIMIT #{MAX_SELECT_ROW};", now) {|row|

          unless row[:created_at]
            # finished/canceled task
            @db["DELETE FROM `#{@table}` WHERE id=?;", row[:id]].delete

          else
            n = @db["UPDATE `#{@table}` SET timeout=? WHERE id=? AND timeout=?;", timeout, row[:id], row[:timeout]].update
            if n > 0
              return row[:id], Task.new(row[:id], row[:created_at], row[:data])
            end
          end

          rows += 1
        }
        if rows < MAX_SELECT_ROW
          return nil
        end
      end
    }
  end

  def finish(id, delete_timeout=3600, now=Time.now.to_i)
    connect {
      n = @db["UPDATE `#{@table}` SET timeout=?, created_at=NULL WHERE id=? AND created_at IS NOT NULL;", now+delete_timeout, id].update
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

  def submit(id, data, time=Time.now.to_i)
    connect {
      begin
        n = @db["INSERT INTO `#{@table}` (id, timeout, data, created_at) VALUES (?, ?, ?, ?);", id, time, data, time].insert
        return true
      rescue Sequel::DatabaseError
        return nil
      end
    }
  end
end


end


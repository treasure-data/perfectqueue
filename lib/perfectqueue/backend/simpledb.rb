
module PerfectQueue


class SimpleDBBackend < Backend
  def initialize(key_id, secret_key, domain)
    require 'aws-sdk'
    @db = AWS::SimpleDB.new(
      :access_key_id => key_id,
      :secret_access_key => secret_key)
    @domain_name = domain
    @domain = @db.domains[@domain_name]
    unless @domain.exists?
      @domain = @db.domains.create(@domain_name)
    end
  end

  def list(&block)
    @domain.items.each {|item|
      id = item.name
      attrs = item.attrs
      created_at = int_decode(attrs['created_at'].first)
      data = attrs['data'].first
      timeout = int_decode(attrs['timeout'].first)
      yield id, created_at, data, timeout
    }
  end

  MAX_SELECT_ROW = 32

  def acquire(timeout, now=Time.now.to_i)
    while true
      rows = 0
      puts "select..."
      @domain.items.select('timeout', 'data', 'created_at',
                          :where => "timeout <= '#{int_encode(now)}'",
                          :order => [:timeout, :asc],
                          :limit => MAX_SELECT_ROW) {|item|
        id = item.name
        attrs = item.attributes
        created_at = int_decode(attrs['created_at'].first)
        data = attrs['data'].first
        # FIXME check result
        item.attributes.replace(:timeout=>int_encode(timeout), :if=>['timeout', attrs[:timeout]])
        puts "#{id} #{created_at} #{data}"
        return id, created_at, data
      }
      if rows < MAX_SELECT_ROW
        return nil
      end
    end
  end

  def finish(id)
    # always nil
    @domain.items[id].delete
  end

  def update(id, timeout)
    begin
      # FIXME check result for cancel
      @domain.items[id].attributes.replace(:timeout=>int_encode(timeout))
    rescue
      raise CanceledError, "Task id=#{id} is canceled."
    end
  end

  def cancel(id)
    finish(id)
  end

  def submit(id, data, time=Time.now.to_i)
    # always nil
    # FIXME check result? unique
    @domain.items[id].attributes.add('timeout'=>int_encode(time), 'created_at'=>int_encode(time), :data=>data)
    nil
  end

  private
  def int_encode(num)
    "%08x" % num
  end

  def int_decode(str)
    str.to_i(16)
  end
end


end


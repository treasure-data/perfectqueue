
module PerfectQueue


class SimpleDBBackend < Backend
  def initialize(key_id, secret_key, domain)
    gem "aws-sdk"
    require 'aws'
    @consistent_read = false

    @db = AWS::SimpleDB.new(
      :access_key_id => key_id,
      :secret_access_key => secret_key)

    @domain_name = domain
    @domain = @db.domains[@domain_name]
    unless @domain.exists?
      @domain = @db.domains.create(@domain_name)
    end
  end

  attr_accessor :consistent_read

  def list(&block)
    @domain.items.each {|item|
      id = item.name
      attrs = item.data.attributes
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
      @domain.items.select('timeout', 'data', 'created_at',
                          :where => "timeout <= '#{int_encode(now)}'",
                          :order => [:timeout, :asc],
                          :consistent_read => @consistent_read,
                          :limit => MAX_SELECT_ROW) {|itemdata|
        begin
          id = itemdata.name
          row = itemdata.attributes
          salt = row['created_at'].first
          @domain.items[id].attributes.replace('timeout'=>int_encode(timeout),
              :if=>{'timeout'=>row['timeout'].first})

          created_at = int_decode(row['created_at'].first)
          data = row['data'].first

          return [id,salt], Task.new(id, created_at, data)

        rescue AWS::SimpleDB::Errors::ConditionalCheckFailed, AWS::SimpleDB::Errors::AttributeDoesNotExist
        end
      }

      if rows < MAX_SELECT_ROW
        return nil
      end
    end
  end

  def finish(token)
    # always nil
    id, salt = *token
    @domain.items[id].delete
  end

  def update(token, timeout)
    begin
      id, salt = *token
      @domain.items[id].attributes.replace('timeout'=>int_encode(timeout),
          :if=>{'created_at'=>salt})
    rescue AWS::SimpleDB::Errors::ConditionalCheckFailed, AWS::SimpleDB::Errors::AttributeDoesNotExist
      raise CanceledError, "Task id=#{id} is canceled."
    end
  end

  def cancel(id)
    begin
      salt = @domain.items[id].attributes['created_at'].first
      unless salt
        return false
      end
      @domain.items[id].delete(:if=>{'created_at'=>salt})
      return true
    rescue AWS::SimpleDB::Errors::ConditionalCheckFailed, AWS::SimpleDB::Errors::AttributeDoesNotExist
      return false
    end
  end

  def submit(id, data, time=Time.now.to_i)
    begin
      @domain.items[id].attributes.replace('timeout'=>int_encode(time), 'created_at'=>int_encode(time), 'data'=>data,
          :unless=>'timeout')
      return true
    rescue AWS::SimpleDB::Errors::ConditionalCheckFailed, AWS::SimpleDB::Errors::ExistsAndExpectedValue
      return nil
    end
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


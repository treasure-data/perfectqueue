$LOAD_PATH << File.expand_path(File.dirname(__FILE__)+"/../lib")
require 'perfectqueue'
require 'perfectqueue/backend/rdb'
require 'perfectqueue/backend/simpledb'

class StressTest
  def initialize(uri, table, npt, thread)
    @db_proc = Proc.new do
      PerfectQueue::RDBBackend.new(uri, table)
    end
    @db_proc.call.create_tables
    @npt = npt
    @thread = thread
  end

  class ThreadMain < Thread
    def initialize(key_prefix, db, num, now)
      @key_prefix = key_prefix
      @db = db
      @num = num
      @now = now
      super(&method(:run))
    end

    def run
      @num.times {|i|
        @db.submit("#{@key_prefix}-#{i}", "data", @now)
        token, task = @db.acquire(@now+1)
        if token == nil
          puts "acquire failed"
          next
        end
        @db.update(token, @now+2)
        @db.finish(token)
      }
    end
  end

  def run
    threads = []
    key_prefix = "stress-#{'%08x'%rand(2**32)}"
    now = Time.now
    @thread.times {|i|
      threads << ThreadMain.new("#{key_prefix}-#{i}", @db_proc.call, @npt, now.to_i)
    }
    threads.each {|t|
      t.join
    }
    finish = Time.now

    elapsed = finish - now
    puts "#{elapsed} sec."
    puts "#{@npt * @thread / elapsed} req/sec."
  end
end

require 'optparse'

op = OptionParser.new
op.banner += " <uri> <table>"

num = 100
thread = 1

op.on('-n', '--num N', Integer) {|n|
  num = n
}
op.on('-t', '--thread N', Integer) {|n|
  thread = n
}

begin
  op.parse!(ARGV)

  if ARGV.length != 2
    puts op.to_s
    exit 1
  end

  uri = ARGV[0]
  table = ARGV[1]

rescue
  puts op.to_s
  puts $!
  exit 1
end

npt = num / thread
num = npt * thread

puts "num: #{num}"
puts "thread: #{thread}"
puts "num/thread: #{npt}"

t = StressTest.new(uri, table, npt, thread)
t.run


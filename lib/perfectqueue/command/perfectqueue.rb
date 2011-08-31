require 'optparse'
require 'perfectqueue/version'

op = OptionParser.new

op.banner += " [-- <ARGV-for-exec-or-run>]"
op.version = PerfectQueue::VERSION

type = nil
id = nil
data = nil
confout = nil

defaults = {
  :timeout => 600,
  :poll_interval => 1,
  :kill_interval => 60,
  :workers => 1,
  :expire => 345600,
}

conf = { }


op.on('-o', '--log PATH', "log file path") {|s|
  conf[:log] = s
}

op.on('-v', '--verbose', "verbose mode", TrueClass) {|b|
  conf[:verbose] = true
}
op.separator("")

op.on('--push ID=DATA', 'Push a task to the queue') {|s|
  type = :push
  id, data = s.split('=',2)
}

op.on('--list', 'Show queued tasks', TrueClass) {|b|
  type = :list
}

op.on('--cancel ID', 'Cancel a queued task') {|s|
  type = :cancel
  id = s
}

op.on('--configure PATH.yaml', 'Write configuration file') {|s|
  type = :conf
  confout = s
}

op.separator("")

op.on('--exec COMMAND', 'Execute command') {|s|
  type = :exec
  conf[:exec] = s
}

op.on('--run SCRIPT.rb', 'Run method named \'run\' defined in the script') {|s|
  type = :run
  conf[:run] = s
}

op.separator("")

op.on('-f', '--file PATH.yaml', 'Read configuration file') {|s|
  (conf[:files] ||= []) << s
}

op.on('-C', '--run-class', 'Class name for --run (default: ::Run)') {|s|
  conf[:run_class] = s
}

op.on('-t', '--timeout SEC', 'Time for another worker to take over a task when this worker goes down (default: 600)', Integer) {|i|
  conf[:timeout] = i
}

op.on('-b', '--heartbeat-interval SEC', 'Threshold time to extend the timeout (heartbeat interval) (default: timeout * 3/4)', Integer) {|i|
  conf[:heartbeat_interval] = i
}

op.on('-x', '--kill-timeout SEC', 'Threshold time to kill a task process (default: timeout * 10)', Integer) {|i|
  conf[:kill_timeout] = i
}

op.on('-X', '--kill-interval SEC', 'Threshold time to retry killing a task process (default: 60)', Integer) {|i|
  conf[:kill_interval] = i
}

op.on('-i', '--poll-interval SEC', 'Polling interval (default: 1)', Integer) {|i|
  conf[:poll_interval] = i
}

op.on('-r', '--retry-wait SEC', 'Time to retry a task when it is failed (default: same as timeout)', Integer) {|i|
  conf[:retry_wait] = i
}

op.on('-e', '--expire SEC', 'Threshold time to expire a task (default: 345600 (4days))', Integer) {|i|
  conf[:expire] = i
}

op.separator("")

op.on('--database URI', 'Use RDBMS for the backend database (e.g.: mysql://user:password@localhost/mydb)') {|s|
  conf[:backend_database] = s
}

op.on('--table NAME', 'backend: name of the table (default: perfectqueue)') {|s|
  conf[:backend_table] = s
}

op.on('--simpledb DOMAIN', 'Use Amazon SimpleDB for the backend database (e.g.: --simpledb mydomain -k KEY_ID -s SEC_KEY)') {|s|
  conf[:backend_simpledb] = s
}

op.on('-k', '--key-id ID', 'AWS Access Key ID') {|s|
  conf[:backend_key_id] = s
}

op.on('-s', '--secret-key KEY', 'AWS Secret Access Key') {|s|
  conf[:backend_secret_key] = s
}

op.separator("")

op.on('-w', '--worker NUM', 'Number of worker threads (default: 1)', Integer) {|i|
  conf[:workers] = i
}

op.on('-d', '--daemon PIDFILE', 'Daemonize (default: foreground)') {|s|
  conf[:daemon] = s
}

op.on('-o', '--log PATH', "log file path") {|s|
  conf[:log] = s
}

op.on('-v', '--verbose', "verbose mode", TrueClass) {|b|
  conf[:verbose] = true
}


(class<<self;self;end).module_eval do
  define_method(:usage) do |msg|
    puts op.to_s
    puts "error: #{msg}" if msg
    exit 1
  end
end


begin
  if eqeq = ARGV.index('--')
    argv = ARGV.slice!(0, eqeq)
    ARGV.slice!(0)
  else
    argv = ARGV.slice!(0..-1)
  end
  op.parse!(argv)

  if argv.length != 0
    usage nil
  end

  if conf[:files]
    require 'yaml'
    docs = ''
    conf[:files].each {|file|
      docs << File.read(file)
    }
    y = {}
    YAML.load_documents(docs) {|yaml|
      yaml.each_pair {|k,v| y[k.to_sym] = v }
    }

    conf = defaults.merge(y).merge(conf)

    if ARGV.empty? && conf[:args]
      ARGV.clear
      ARGV.concat conf[:args]
    end
  else
    conf = defaults.merge(conf)
  end

  unless type
    if conf[:run]
      type = :run
    elsif conf[:exec]
      type = :exec
    else
      raise "--list, --push, --cancel, --configure, --exec or --run is required"
    end
  end

  unless conf[:heartbeat_interval]
    conf[:heartbeat_interval] = conf[:timeout] * 3/4
  end

  unless conf[:kill_timeout]
    conf[:kill_timeout] = conf[:timeout] * 10
  end

  unless conf[:retry_wait]
    conf[:retry_wait] = conf[:timeout]
  end

  if conf[:timeout] < conf[:heartbeat_interval]
    raise "--heartbeat-interval(=#{conf[:heartbeat_interval]}) must be larger than --timeout(=#{conf[:timeout]})"
  end

  if conf[:backend_database]
    conf[:backend_table] ||= 'perfectqueue'
    backend_proc = Proc.new {
      PerfectQueue::RDBBackend.new(conf[:backend_database], conf[:backend_table])
    }
  elsif conf[:backend_simpledb]
    conf[:backend_key_id] ||= ENV['AWS_ACCESS_KEY_ID']
    conf[:backend_secret_key] ||= ENV['AWS_SECRET_ACCESS_KEY']
    backend_proc = Proc.new {
      PerfectQueue::SimpleDBBackend.new(conf[:backend_key_id], conf[:backend_secret_key], conf[:backend_simpledb])
    }

  else
    raise "--database or --simpledb is required"
  end

rescue
  usage $!.to_s
end


if confout
  require 'yaml'

  conf.delete(:file)
  conf[:args] = ARGV

  y = {}
  conf.each_pair {|k,v| y[k.to_s] = v }

  File.open(confout, "w") {|f|
    f.write y.to_yaml
  }
  exit 0
end


require 'logger'
require 'perfectqueue'
require 'perfectqueue/backend/rdb'
require 'perfectqueue/backend/simpledb'

backend = backend_proc.call

case type
when :list
  format = "%26s %26s %26s  %s"
  puts format % ["id", "created_at", "timeout", "data"]
  n = 0
  backend.list {|id,created_at,data,timeout|
    puts format % [id, Time.at(created_at), Time.at(timeout), data]
    n += 1
  }
  puts "#{n} entries."

when :cancel
  canceled = backend.cancel(id)
  if canceled
    puts "Task id=#{id} is canceled."
  else
    puts "Task id=#{id} does not exist. abort"
  end

when :push
  submitted = backend.submit(id, data, Time.now.to_i)
  if submitted
    puts "Task id=#{id} is submitted."
  else
    puts "Task id=#{id} is duplicated. abort."
  end

when :exec, :run
  if conf[:daemon]
    exit!(0) if fork
    Process.setsid
    exit!(0) if fork
    File.umask(0)
    STDIN.reopen("/dev/null")
    STDOUT.reopen("/dev/null", "w")
    STDERR.reopen("/dev/null", "w")
    File.open(conf[:daemon], "w") {|f|
      f.write Process.pid.to_s
    }
  end

  if type == :run
    load File.expand_path(conf[:run])
    run_class = eval(conf[:run_class] || 'Run')
  else
    require 'shellwords'
    cmd = ARGV.map {|a| Shellwords.escape(a) }.join(' ')
    Run = Class.new(PerfectQueue::ExecRunner) do
      define_method(:initialize) {|task|
        super(cmd, task)
      }
    end
    run_class = Run
  end

  conf[:run_class] = run_class

  if log_file = conf[:log]
    log_out = File.open(conf[:log], "a")
  else
    log_out = STDOUT
  end

  log = Logger.new(log_out)
  if conf[:verbose]
    log.level = Logger::DEBUG
  else
    log.level = Logger::INFO
  end

  engine = PerfectQueue::Engine.new(backend, log, conf)

  trap :INT do
    log.info "shutting down..."
    engine.stop
  end

  trap :TERM do
    log.info "shutting down..."
    engine.stop
  end

  trap :HUP do
    if log_file
      log_out.reopen(log_file, "a")
    end
  end

  log.info "PerfectQueue-#{PerfectQueue::VERSION}"

  begin
    engine.run
    engine.shutdown
  rescue
    log.error $!.to_s
    $!.backtrace.each {|x|
      log.error "  #{x}"
    }
    exit 1
  end
end


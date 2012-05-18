require 'optparse'
require 'perfectqueue/version'

op = OptionParser.new

op.banner += %[ <command>

commands:
    list                             Show list of tasks
    submit <key> <type> <data>       Submit a new task
    cancel_request <key>             Cancel request
    force_finish <key>               Force finish a task
    run <class>                      Run a worker process
    init                             Initialize a backend database

]
op.version = PerfectQueue::VERSION

env = ENV['RAILS_ENV'] || 'development'
config_path = 'config/perfectqueue.yml'
include_dirs = []
require_files = []

task_options = {
}

op.separator("options:")

op.on('-e', '--environment ENV', 'Framework environment (default: development)') {|s|
  env = s
}

op.on('-c', '--config PATH.yml', 'Path to a configuration file (default: config/perfectqueue.yml)') {|s|
  config_path = s
}

op.separator("\noptions for submit:")

op.on('-u', '--user USER', 'Set user') {|s|
  task_options[:user] = s
}

op.on('-t', '--time UNIXTIME', 'Set time to run the task', Integer) {|i|
  task_options[:run_at] = i
}


op.separator("\noptions for run:")

op.on('-I', '--include PATH', 'Add $LOAD_PATH directory') {|s|
  include_dirs << s
}

op.on('-r', '--require PATH', 'Require files before starting') {|s|
  require_files << s
}

(class<<self;self;end).module_eval do
  define_method(:usage) do |msg|
    puts op.to_s
    puts "\nerror: #{msg}" if msg
    exit 1
  end
end

begin
  op.parse!(ARGV)

  usage nil if ARGV.empty?

  cmd = ARGV.shift
  case cmd
  when 'list'
    cmd = :list
    usage nil unless ARGV.length == 0

  when 'cancel_request' ,'cancel'
    cmd = :cancel
    usage nil unless ARGV.length == 1
    key = ARGV[0]

  when 'force_finish' ,'finish'
    cmd = :finish
    usage nil unless ARGV.length == 1
    key = ARGV[0]

  when 'submit'
    cmd = :submit
    usage nil unless ARGV.length == 3
    key, type, data = *ARGV
    require 'json'
    data = JSON.load(data)

  when 'run'
    cmd = :run
    usage nil unless ARGV.length == 1
    klass = ARGV[0]

  when 'init'
    cmd = :init
    usage nil unless ARGV.length == 0

  else
    raise "unknown command: '#{cmd}'"
  end

rescue
  usage $!.to_s
end

require 'yaml'
require 'perfectqueue'

config_load_proc = Proc.new {
  yaml = YAML.load(File.read(config_path))
  conf = yaml[env]
  unless conf
    raise "Configuration file #{config_path} doesn't include configuration for environment '#{env}'"
  end
  conf
}


case cmd
when :list
  n = 0
  PerfectQueue.open(config_load_proc.call) {|queue|
    format = "%30s %10s %18s %18s %28s %28s   %s"
    puts format % ["key", "type", "user", "status", "created_at", "timeout", "data"]
    queue.each {|task|
      puts format % [task.key, task.type, task.user, task.status, task.created_at, task.timeout, task.data]
      n += 1
    }
  }
  puts "#{n} entries."

when :cancel
  PerfectQueue.open(config_load_proc.call) {|queue|
    queue[key].cancel_request!
  }

when :finish
  PerfectQueue.open(config_load_proc.call) {|queue|
    queue[key].force_finish!
  }

when :submit
  PerfectQueue.open(config_load_proc.call) {|queue|
    queue.submit(key, type, data, task_options)
  }

when :run
  include_dirs.each {|path|
    $LOAD_PATH << File.expand_path(path)
  }
  require_files.each {|file|
    require file
  }
  klass = Object.const_get(klass)
  PerfectQueue::Worker.run(klass, &config_load_proc)

when :init
  PerfectQueue.open(config_load_proc.call) {|queue|
    queue.client.init_database
  }
end


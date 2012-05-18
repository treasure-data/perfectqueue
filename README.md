# PerfectQueue

PerfectQueue is a highly available distributed queue built on top of RDBMS.
It provides at-least-once semantics; Even if a worker node fails during processing a task, the task is retried by another worker.
PerfectQueue provides similar API to Amazon SQS. But unlike Amazon SQS, PerfectQueue never delivers finished tasks.

Since PerfectQueue also prevents storing a same task twice by using unique task identifier, client applications can retry to submit tasks until it succeeds.

All you have to consider is implementing idempotent worker programs. PerfectQueue manages the other problems.

## API overview

```
# open a queue
PerfectQueue.open(config, &block)  #=> #<Queue>

# submit a task
Queue#submit(task_id, type, data, options={})

# poll a task
# (you don't have to use this method directly. see following sections)
Queue#poll  #=> #<AcquiredTask>

# get data associated with a task
AcquiredTask#data  #=> #<Hash>

# finish a task
AcquiredTask#finish!

# retry a task
AcquiredTask#retry!
```

###  Example

```ruby
# submit tasks
PerfectQueue.open(config) {|queue|
  data = {'key'=>"value"}
  queue.submit("task-id", "type1", data)
}
```


## Writing a worker application

### 1. Implement PerfectQueue::Application::Base

```ruby
class TestHandler < PerfectQueue::Application::Base
  # implement run method
  def run
    # do something ...
    puts "acquired task: #{task.inspect}"

    # call task.finish!, task.retry! or task.release!
    task.finish!
  end
end
```

### 2. Implement PerfectQueue::Application::Dispatch

```ruby
class Dispatch < PerfectQueue::Application::Dispatch
  # describe routing
  route "type1" => TestHandler
  route /^regexp-.*$/ => :TestHandler  # String or Regexp => Class or Symbol
end
```

### 3. Run the worker

In a launcher script or rake file:

```ruby
system('perfectsched run -I. -rapp/workers/dispatch Dispatch')
```

or:

```ruby
request 'perfectqueue'
require 'app/workers/dispatch'

PerfectQueue::Worker.run(Dispatch) {
  # this method is called when the worker process is restarted
  raw = File.read('config/perfectqueue.yml')
  yml = YAJL.load(raw)
  yml[ENV['RAILS_ENV'] || 'development']
}
```

### Signal handlers

- **TERM,INT:** graceful shutdown
- **QUIT:** immediate shutdown
- **USR1:** graceful restart
- **HUP:** immediate restart
- **WINCH:** immediate binary replace
- **CONT:** graceful binary replace
- **USR2:** reopen log files

## Configuration

- **type:** backend type (required; see following sections)
- **log:** log file path (default: use stderr)
- **processors:** number of child processes (default: 1)
- **poll_interval:** interval to poll tasks in seconds (default: 1.0 sec)
- **retention_time:** duration to retain finished tasks (default: 300 sec)
- **task_heartbeat_interval:** interval to send heartbeat requests (default: 2 sec)
- **alive_time:** duration to continue a heartbeat request (default: 300 sec)
- **retry_wait:** duration to retry a retried task (default: 300 sec)
- **child_kill_interval:** interval to send signals to a child process (default: 2.0 sec)
- **child_graceful_kill_limit:** threshold time to switch SIGTERM to SIGKILL (default: never)
- **child_heartbeat_interval:** interval to send heartbeat packets to a child process (default: 2 sec)
- **child_heartbeat_limit:** threshold time to detect freeze of a child process (default: 10.0 sec)

## Backend types

### rdb\_compat

additional configuration:

- **url:** URL to the RDBMS (example: 'mysql://user:password@host:port/database')
- **table:** name of the table to use


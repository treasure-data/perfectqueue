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

# create a task reference
Queue#[](key)  #=> #<Task>

# chack the existance of the task
Task#exists?

# request to cancel a task
# (actual behavior depends on the worker program)
Task#cancel_request!

# force finish a task
# be aware that worker programs can't detect it
Task#force_finish!
```

### Error classes

```
TaskError

##
# Workers may get these errors:
#

CancelRequestedError < TaskError

AlreadyFinishedError < TaskError

PreemptedError < TaskError

ProcessStopError < RuntimeError

ImmediateProcessStopError < ProcessStopError

GracefulProcessStopError < ProcessStopError

##
# Client or other situation:
#

ConfigError < RuntimeError

NotFoundError < TaskError

AlreadyExistsError < TaskError

NotSupportedError < TaskError
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
system('perfectqueue run -I. -rapp/workers/dispatch Dispatch')
```

or:

```ruby
require 'perfectqueue'
require 'app/workers/dispatch'

PerfectQueue::Worker.run(Dispatch) {
  # this method is called when the worker process is restarted
  raw = File.read('config/perfectqueue.yml')
  yml = YAJL.load(raw)
  yml[ENV['RAILS_ENV'] || 'development']
}
```

### Signal handlers

- **TERM**,**INT:** graceful shutdown
- **QUIT:** immediate shutdown
- **USR1:** graceful restart
- **HUP:** immediate restart
- **EMT:** immediate binary replace
- **WINCH:** graceful binary replace
- **USR2:** reopen log files

## Configuration

- **type:** backend type (required; see following sections)
- **log:** log file path (default: use stderr)
- **processors:** number of child processes (default: 1)
- **processor_type:** type of processor ('process' or 'thread') (default: 'process')
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

### rdb

Not implemented yet.


## Command line management tool

```
Usage: perfectqueue [options] <command>

commands:
    list                             Show list of tasks
    submit <key> <type> <data>       Submit a new task
    cancel_request <key>             Cancel request
    force_finish <key>               Force finish a task
    run <class>                      Run a worker process
    init                             Initialize a backend database

options:
    -e, --environment ENV            Framework environment (default: development)
    -c, --config PATH.yml            Path to a configuration file (default: config/perfectqueue.yml)

options for submit:
    -u, --user USER                  Set user
    -t, --time UNIXTIME              Set time to run the task

options for run:
    -I, --include PATH               Add $LOAD_PATH directory
    -r, --require PATH               Require files before starting
```


### initializing a database

    # assume that the config/perfectqueue.yml exists
    $ perfectqueue init

### submitting a task

    $ perfectqueue submit k1 user_task '{"uid":1}' -u user_1

### listing tasks

    $ perfectqueue list
                               key            type               user             status                   created_at                      timeout   data
                                k1       user_task             user_1            waiting    2012-05-18 13:05:31 -0700    2012-05-18 14:27:36 -0700   {"uid"=>1, "type"=>"user_task"}
                                k2       user_task             user_2            waiting    2012-05-18 13:35:33 -0700    2012-05-18 14:35:33 -0700   {"uid"=>2, "type"=>"user_task"}
                                k3     system_task                               waiting    2012-05-18 14:04:02 -0700    2012-05-22 15:04:02 -0700   {"task_id"=>32, "type"=>"system_task"}
    3 entries.

### cancel a tasks

    $ perfectqueue cancel_request k1

### force finish a tasks

    $ perfectqueue cancel_request k2

### running a worker

    $ perfectqueue run -I. -Ilib -rconfig/boot.rb -rapps/workers/task_dispatch.rb TaskDispatch


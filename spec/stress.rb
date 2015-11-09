require 'spec_helper'

describe Queue do
  include QueueTest

  let :thread_num do
    5
  end

  let :loop_num do
    50
  end

  let :now do
    Time.now.to_i
  end

  def thread_main
    thread_id = Thread.current.object_id

    loop_num.times do |i|
      queue.submit("#{thread_id}-#{i}", "type01", {}, :now=>now-10)
      task = queue.poll(:now=>now, :alive_time=>60)
      expect(task).not_to eq(nil)
      task.heartbeat!(:now=>now, :alive_time=>70)
      task.finish!(:now=>now, :retention_time=>80)
    end
  end

  it 'stress' do
    puts "stress test with threads=#{thread_num} * loop_num=#{loop_num} = #{thread_num * loop_num} tasks"

    # initialize queue here
    queue
    now

    start_at = Time.now
    (1..thread_num).map {
      Thread.new(&method(:thread_main))
    }.each {|thread|
      thread.join
    }
    finish_at = Time.now

    elapsed = finish_at - start_at
    task_num = thread_num * loop_num
    puts "#{elapsed} sec."
    puts "#{task_num / elapsed} req/sec."
    puts "#{elapsed / task_num} sec/req."
  end

end


require File.dirname(__FILE__)+'/test_helper'

class ExecTest < Test::Unit::TestCase
  it 'success' do
    success_sh  = File.expand_path File.dirname(__FILE__)+"/success.sh"

    task = PerfectQueue::Task.new('test1', Time.now.to_i, 'data1')
    e = PerfectQueue::ExecRunner.new(success_sh, task)

    assert_nothing_raised do
      e.run
    end
  end

  it 'fail' do
    fail_sh  = File.expand_path File.dirname(__FILE__)+"/fail.sh"

    task = PerfectQueue::Task.new('test1', Time.now.to_i, 'data1')
    e = PerfectQueue::ExecRunner.new(fail_sh, task)

    assert_raise(RuntimeError) do
      e.run
    end
  end

  it 'stdin' do
    cat_sh  = File.expand_path File.dirname(__FILE__)+"/cat.sh"
    out_tmp = File.expand_path File.dirname(__FILE__)+"/cat.sh.tmp"

    task = PerfectQueue::Task.new('test1', Time.now.to_i, 'data1')
    e = PerfectQueue::ExecRunner.new("#{cat_sh} #{out_tmp}", task)

    e.run

    assert_equal 'data1', File.read(out_tmp)
  end

  it 'echo' do
    echo_sh  = File.expand_path File.dirname(__FILE__)+"/echo.sh"
    out_tmp = File.expand_path File.dirname(__FILE__)+"/echo.sh.tmp"

    task = PerfectQueue::Task.new('test1', Time.now.to_i, 'data1')
    e = PerfectQueue::ExecRunner.new("#{echo_sh} #{out_tmp}", task)

    e.run

    assert_equal "test1\n", File.read(out_tmp)
  end

  it 'huge' do
    huge_sh  = File.expand_path File.dirname(__FILE__)+"/huge.sh"

    task = PerfectQueue::Task.new('test1', Time.now.to_i, 'data1')
    e = PerfectQueue::ExecRunner.new(huge_sh, task)

    e.run

    # should finish
  end
end


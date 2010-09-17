require 'spec_helper'
require 'delayed/master'

APP_ROOT = Pathname.new('..').expand_path(__FILE__)

describe Delayed::Master do
  before do
    ActiveRecord::Base.verify_active_connections!
    Delayed::Job.before_fork
    @master = Delayed::Master.new
  end

  after do
    @master.stop if @master.running?
  end

  describe "start" do
    it "should write a pid file" do
      @master.start
      @master.pid_file.should be_file
      @master.pid.should_not == $$
    end

    it "should create pid dir" do
      @master.pid_file.dirname.rmtree
      @master.start
      @master.pid_file.dirname.directory?.should be_true
    end

    context "with a stale pid file" do
      before do
        @pid = fork {}
        Process.wait(@pid)
        @master.pid_file.open('w') {|f| f << @pid }
      end

      it "should overwrite pid file" do
        @master.start
        @master.pid_file.read.to_i.should_not == @pid
      end
    end

    it "should exit with status of 1 if worker is running" do
      @master.start
      pid = @master.pid_file.read
      silence_stderr { Process.wait(fork { @master.start }) }
      $?.exitstatus.should == 1
      @master.pid_file.read.should == pid
    end
  end

  describe "stop" do
    context "when no pid file exists" do
      it "should abort" do
        silence_stderr { Process.wait(fork { @master.stop }) }
        $?.exitstatus.should == 1
      end
    end

    context "when the process has died" do
      before do
        # get an unused pid
        pid = fork {}
        Process.wait(pid)

        @master.pid_file.open('w') {|f| f << pid }

        silence_stderr { Process.wait(fork { @master.stop }) }
      end

      it "should abort" do
        $?.exitstatus.should == 1
      end
    end

    context "when the process is running" do
      before do
        @master.start
      end

      it "should kill the process and delete the pid file" do
        @master.should be_running
        @master.pid_file.should be_file
        @master.stop
        @master.should_not be_running
        wait_until { @master.pid_file.should_not be_file }
      end
    end
  end

  describe "root" do
    it "should use pwd if not specified" do
      Dir.chdir '/tmp' do
        Delayed::Master.new.root.should == Pathname.new(Dir.pwd)
      end
    end

    it "should use :root option" do
      Delayed::Master.new(:root => '/tmp').root.should == Pathname.new('/tmp')
    end
  end

  describe "spawn_worker" do
    it "should find and run a job" do
      job = Delayed::Job.enqueue SimpleJob.new
      Process.wait(@master.spawn_worker(0))
      Delayed::Job.count.should == 0
      Delayed::Job.exists?(job.id).should be_false
    end

    it "should not run if a job does not exist" do
      @master.should_not_receive(:fork)
      @master.spawn_worker(0)
    end
  end

  def wait_until(timeout = 5)
    error = nil

    Timeout::timeout(timeout) do
      begin
        yield
      rescue => error
        sleep 0.5
        retry
      end
    end
  rescue Timeout::Error => timeout
    raise error || timeout
  end
end
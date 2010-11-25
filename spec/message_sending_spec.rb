require 'spec_helper'

describe Delayed::MessageSending do
  class Delayable
    cattr_accessor :type

    def omg!(arg)
    end
    handle_asynchronously :omg!, :queue => 'srsly'

    def proc
    end
    handle_asynchronously :proc, :queue => Proc.new { self.type }

    def proc_with_arg
    end
    handle_asynchronously :proc_with_arg, :queue => Proc.new {|m| m.type }
  end

  describe "handle_asynchronously" do
    it "should alias original method" do
      Delayable.new.should respond_to(:omg_without_delay!)
      Delayable.new.should respond_to(:omg_with_delay!)
    end

    it "should create a job" do
      job = Delayable.new.omg!(1)
      job.payload_object.class.should == Delayed::PerformableMethod
      job.payload_object.method_name.should == :omg_without_delay!
      job.payload_object.args.should == [1]
    end

    it "should set options" do
      job = Delayable.new.omg!(1)
      job.queue.should == 'srsly'
    end

    it "should set the options based on result of proc" do
      d = Delayable.new
      d.type = 'with_a_proc'
      job = d.proc
      job.queue.should == 'with_a_proc'
    end

    it "should set the options based on result of proc with arg" do
      d = Delayable.new
      d.type = 'arg'
      job = d.proc_with_arg
      job.queue.should == 'arg'
    end
  end

  context "delay" do
    it "should create a new PerformableMethod job" do
      lambda {
        job = "hello".delay.count('l')
        job.payload_object.class.should   == Delayed::PerformableMethod
        job.payload_object.method_name.should  == :count
        job.payload_object.args.should    == ['l']
      }.should change { Delayed::Job.count }.by(1)
    end

    it "should set job options" do
      run_at = Time.parse('2010-05-03 12:55 AM')
      job = Object.delay(:queue => 'foo', :run_at => run_at).to_s
      job.run_at.should == run_at
      job.queue.should == 'foo'
    end
  end
end

require File.expand_path('../../../../spec/sample_jobs', __FILE__)

shared_examples_for 'a delayed_job backend' do
  let(:worker) { Delayed::Worker.new }

  def described_class
    @described_class ||= Recorder.new(super)
  end

  def create_job(opts = {})
    described_class.create({:payload_object => SimpleJob.new}.merge(opts))
  end

  class Recorder < ActiveSupport::BasicObject
    cattr_accessor :instances
    self.instances = []
    attr_reader :called_methods

    def initialize(target)
      @target = target
      @called_methods = []
      self.instances << self
    end

    def method_missing(method, *args)
      @called_methods << method
      @target.send(method, *args)
    end
  end

  before do
    SimpleJob.runs = 0
    described_class.delete_all
  end

  BACKEND_CLASS_METHODS = [:reserve, :enqueue, :create, :delete_all, :db_time_now, :clear_locks!]

  after do
    instances = Recorder.instances.dup
    Recorder.instances.clear
    instances.each do |recorder|
      recorder.called_methods.each do |method|
        BACKEND_CLASS_METHODS.should include(method)
      end
    end
  end

  it "should set run_at automatically if not set" do
    described_class.create(:payload_object => ErrorJob.new ).run_at.should_not be_nil
  end

  it "should not set run_at automatically if already set" do
    later = described_class.db_time_now + 5.minutes
    job = described_class.create(:payload_object => ErrorJob.new, :run_at => later)
    job.run_at.should be_within(1).of(later)
  end

  describe "enqueue" do
    context "with a hash" do
      it "should raise ArgumentError when handler doesn't respond_to :perform" do
        lambda { described_class.enqueue(:payload_object => Object.new) }.should raise_error(ArgumentError)
      end

      it "should be able to set a queue" do
        job = described_class.enqueue :payload_object => SimpleJob.new, :queue => 'emails'
        job.queue.should == 'emails'
      end

      it "should set default queue to nil" do
        job = described_class.enqueue :payload_object => SimpleJob.new
        job.queue.should be_nil
      end

      it "should be able to set run_at" do
        later = described_class.db_time_now + 5.minutes
        job = described_class.enqueue :payload_object => SimpleJob.new, :run_at => later
        job.run_at.should be_within(1).of(later)
      end
    end

    context "with multiple arguments" do
      it "should raise ArgumentError when handler doesn't respond_to :perform" do
        lambda { described_class.enqueue(Object.new) }.should raise_error(ArgumentError)
      end

      it "should enqueue job for objects that respond to :perform" do
        job = described_class.enqueue SimpleJob.new
        described_class.reserve(worker).should == job
      end

      it "should work with jobs in modules" do
        M::ModuleJob.runs = 0
        job = described_class.enqueue M::ModuleJob.new
        lambda { job.invoke_job }.should change { M::ModuleJob.runs }.from(0).to(1)
      end
    end
  end

  describe "callbacks" do
    before(:each) do
      CallbackJob.messages = []
    end

    %w(before success after).each do |callback|
      it "should call #{callback} with job" do
        job = described_class.enqueue(CallbackJob.new)
        job.payload_object.should_receive(callback).with(job)
        job.invoke_job
      end
    end

    it "should call before and after callbacks" do
      job = described_class.enqueue(CallbackJob.new)
      CallbackJob.messages.should == ["enqueue"]
      job.invoke_job
      CallbackJob.messages.should == ["enqueue", "before", "perform", "success", "after"]
    end

    it "should call the after callback with an error" do
      job = described_class.enqueue(CallbackJob.new)
      job.payload_object.should_receive(:perform).and_raise(RuntimeError.new("fail"))

      lambda { job.invoke_job }.should raise_error
      CallbackJob.messages.should == ["enqueue", "before", "error: RuntimeError", "after"]
    end

    it "should call error when before raises an error" do
      job = described_class.enqueue(CallbackJob.new)
      job.payload_object.should_receive(:before).and_raise(RuntimeError.new("fail"))
      lambda { job.invoke_job }.should raise_error(RuntimeError)
      CallbackJob.messages.should == ["enqueue", "error: RuntimeError", "after"]
    end
  end

  describe "payload_object" do
    it "should raise a DeserializationError when the job class is totally unknown" do
      job = described_class.create :handler => "--- !ruby/object:JobThatDoesNotExist {}"
      lambda { job.payload_object }.should raise_error(Delayed::DeserializationError)
    end

    it "should raise a DeserializationError when the job struct is totally unknown" do
      job = described_class.create :handler => "--- !ruby/struct:StructThatDoesNotExist {}"
      lambda { job.payload_object }.should raise_error(Delayed::DeserializationError)
    end

    it "should raise a DeserializationError when the YAML.load raises argument error" do
      create_job
      job = described_class.reserve(worker)
      YAML.should_receive(:load).and_raise(ArgumentError)
      lambda { job.payload_object }.should raise_error(Delayed::DeserializationError)
    end
  end

  describe "reserve" do
    before do
      Delayed::Worker.max_run_time = 2.minutes
    end

    it "should not reserve failed jobs" do
      create_job :attempts => 50, :failed_at => described_class.db_time_now
      described_class.reserve(worker).should be_nil
    end

    it "should not reserve jobs scheduled for the future" do
      create_job :run_at => described_class.db_time_now + 1.minute
      described_class.reserve(worker).should be_nil
    end

    it "should not reserve jobs locked by other workers" do
      job = create_job
      other_worker = Delayed::Worker.new
      other_worker.name = 'other_worker'
      described_class.reserve(other_worker).should == job
      described_class.reserve(worker).should be_nil
    end

    it "should reserve open jobs" do
      job = create_job
      described_class.reserve(worker).should == job
    end

    it "should reserve expired jobs" do
      job = create_job(:locked_by => worker.name, :locked_at => described_class.db_time_now - 3.minutes)
      described_class.reserve(worker).should == job
    end

    it "should reserve own jobs" do
      job = create_job(:locked_by => worker.name, :locked_at => (described_class.db_time_now - 1.minutes))
      described_class.reserve(worker).should == job
    end

    context "with specific queues specified" do
      before(:each) do
        worker.queues = ['queue2', 'queue1']
      end

      it "should fetch jobs from first queue" do
        create_job :queue => 'queue1'
        job = create_job :queue => 'queue2'
        described_class.reserve(worker).should == job
      end

      it "should fetch jobs from other queues" do
        job = create_job :queue => 'queue1'
        described_class.reserve(worker).should == job
      end

      it "should not fetch jobs from queues not listed" do
        create_job :queue => 'queue3'
        described_class.reserve(worker).should be_nil
      end
    end

    context "with queues and * specified" do
      before(:each) do
        worker.queues = ['queue1', '*']
      end

      it "should fetch jobs from first queue" do
        create_job(:run_at => 10.seconds.ago)
        job = create_job :queue => 'queue1', :run_at => 1.second.ago
        described_class.reserve(worker).should == job
      end

      it "should fetch jobs not in a queue" do
        job = create_job :queue => nil
        described_class.reserve(worker).should == job
      end
    end

    context "without queues specified" do
      before(:each) do
        worker.queues = nil
      end

      it "should fetch first scheduled job" do
        now = described_class.db_time_now
        create_job :run_at => now - 10, :queue => 'queue1'
        job = create_job :run_at => now - 20, :queue => 'queue2'
        create_job :run_at => now
        described_class.reserve(worker).should == job
      end

      it "should fetch jobs in a queue" do
        job = create_job :queue => 'queue1'
        described_class.reserve(worker).should == job
      end

      it "should fetch jobs not in a queue" do
        job = create_job :queue => nil
        described_class.reserve(worker).should == job
      end
    end
  end

  context "#name" do
    it "should be the class name of the job that was enqueued" do
      create_job(:payload_object => ErrorJob.new ).name.should == 'ErrorJob'
    end

    it "should be the method that will be called if its a performable method object" do
      job = create_job(:payload_object => NamedJob.new)
      job.name.should == 'named_job'
    end

    it "should be the instance method that will be called if its a performable method object" do
      @job = Story.create(:text => "...").delay.save
      @job.name.should == 'Story#save'
    end

    it "should parse from handler on deserialization error" do
      job = Story.create(:text => "...").delay.text
      job.payload_object.object.destroy
      job = described_class.reserve(worker)
      job.name.should == 'Delayed::PerformableMethod'
    end
  end

  context "clear_locks!" do
    before do
      @job = create_job(:locked_by => 'worker1', :locked_at => described_class.db_time_now)
    end

    it "should clear locks for the given worker" do
      described_class.clear_locks!('worker1')
      described_class.reserve(worker).should == @job
    end

    it "should not clear locks for other workers" do
      described_class.clear_locks!('different_worker')
      described_class.reserve(worker).should_not == @job
    end
  end

  context "unlock" do
    before do
      @job = create_job(:locked_by => 'worker', :locked_at => described_class.db_time_now)
    end

    it "should clear locks" do
      @job.unlock
      @job.locked_by.should be_nil
      @job.locked_at.should be_nil
    end
  end

  context "large handler" do
    before do
      text = "Lorem ipsum dolor sit amet. " * 1000
      @job = described_class.enqueue Delayed::PerformableMethod.new(text, :length, {})
    end

    it "should have an id" do
      @job.id.should_not be_nil
    end
  end

  describe "yaml serialization" do
    it "should reload changed attributes" do
      story = Story.create(:text => 'hello')
      job = story.delay.tell
      story.update_attributes :text => 'goodbye'
      described_class.reserve(worker).payload_object.object.text.should == 'goodbye'
    end

    it "should raise deserialization error for destroyed records" do
      story = Story.create(:text => 'hello')
      job = story.delay.tell
      story.destroy
      lambda {
        described_class.reserve(worker).payload_object
      }.should raise_error(Delayed::DeserializationError)
    end
  end

  describe "worker integration" do
    before do
      Delayed::Job.delete_all
      SimpleJob.runs = 0
    end

    describe "running a job" do
      it "should fail after Worker.max_run_time" do
        begin
          old_max_run_time = Delayed::Worker.max_run_time
          Delayed::Worker.max_run_time = 1.second
          @job = Delayed::Job.create :payload_object => LongRunningJob.new
          worker.run(@job)
          @job.reload.last_error.should =~ /expired/
          @job.attempts.should == 1
        ensure
          Delayed::Worker.max_run_time = old_max_run_time
        end
      end

      context "when the job raises a deserialization error" do
        it "should mark the job as failed" do
          Delayed::Worker.destroy_failed_jobs = false
          job = create_job :handler => "--- !ruby/object:JobThatDoesNotExist {}"
          worker.work_off
          job.reload
          job.failed_at.should_not be_nil
        end
      end
    end

    describe "failed jobs" do
      before do
        # reset defaults
        Delayed::Worker.destroy_failed_jobs = true
        Delayed::Worker.max_attempts = 25

        @job = Delayed::Job.enqueue(ErrorJob.new)
      end

      it "should record last_error when destroy_failed_jobs = false, max_attempts = 1" do
        Delayed::Worker.destroy_failed_jobs = false
        Delayed::Worker.max_attempts = 1
        worker.run(@job)
        @job.reload
        @job.last_error.should =~ /did not work/
        @job.attempts.should == 1
        @job.failed_at.should_not be_nil
      end

      it "should re-schedule jobs after failing" do
        worker.work_off
        @job.reload
        @job.last_error.should =~ /did not work/
        @job.last_error.should =~ /sample_jobs.rb:\d+:in `perform'/
        @job.attempts.should == 1
        @job.run_at.should > Delayed::Job.db_time_now - 10.minutes
        @job.run_at.should < Delayed::Job.db_time_now + 10.minutes
        @job.locked_by.should be_nil
        @job.locked_at.should be_nil
      end

      it 'should re-schedule with handler provided time if present' do
        @job = Delayed::Job.enqueue(CustomRescheduleJob.new(99.minutes))
        worker.run(@job)
        @job.reload

        (Delayed::Job.db_time_now + 99.minutes - @job.run_at).abs.should < 1
      end

      it "should not fail when the triggered error doesn't have a message" do
        error_with_nil_message = StandardError.new
        error_with_nil_message.stub!(:message).and_return nil
        @job.stub!(:invoke_job).and_raise error_with_nil_message
        lambda{worker.run(@job)}.should_not raise_error
      end
    end

    context "reschedule" do
      before do
        @job = Delayed::Job.create :payload_object => SimpleJob.new
      end

      share_examples_for "any failure more than Worker.max_attempts times" do
        context "when the job's payload has a #failure hook" do
          before do
            @job = Delayed::Job.create :payload_object => OnPermanentFailureJob.new
            @job.payload_object.should respond_to :failure
          end

          it "should run that hook" do
            @job.payload_object.should_receive :failure
            Delayed::Worker.max_attempts.times { worker.reschedule(@job) }
          end
        end

        context "when the job's payload has no #failure hook" do
          # It's a little tricky to test this in a straightforward way,
          # because putting a should_not_receive expectation on
          # @job.payload_object.failure makes that object
          # incorrectly return true to
          # payload_object.respond_to? :failure, which is what
          # reschedule uses to decide whether to call failure.
          # So instead, we just make sure that the payload_object as it
          # already stands doesn't respond_to? failure, then
          # shove it through the iterated reschedule loop and make sure we
          # don't get a NoMethodError (caused by calling that nonexistent
          # failure method).

          before do
            @job.payload_object.should_not respond_to(:failure)
          end

          it "should not try to run that hook" do
            lambda do
              Delayed::Worker.max_attempts.times { worker.reschedule(@job) }
            end.should_not raise_exception(NoMethodError)
          end
        end
      end

      context "and we want to destroy jobs" do
        before do
          Delayed::Worker.destroy_failed_jobs = true
        end

        it_should_behave_like "any failure more than Worker.max_attempts times"

        it "should be destroyed if it failed more than Worker.max_attempts times" do
          @job.should_receive(:destroy)
          Delayed::Worker.max_attempts.times { worker.reschedule(@job) }
        end

        it "should not be destroyed if failed fewer than Worker.max_attempts times" do
          @job.should_not_receive(:destroy)
          (Delayed::Worker.max_attempts - 1).times { worker.reschedule(@job) }
        end
      end

      context "and we don't want to destroy jobs" do
        before do
          Delayed::Worker.destroy_failed_jobs = false
        end

        it_should_behave_like "any failure more than Worker.max_attempts times"

        it "should be failed if it failed more than Worker.max_attempts times" do
          @job.reload.failed_at.should == nil
          Delayed::Worker.max_attempts.times { worker.reschedule(@job) }
          @job.reload.failed_at.should_not == nil
        end

        it "should not be failed if it failed fewer than Worker.max_attempts times" do
          (Delayed::Worker.max_attempts - 1).times { worker.reschedule(@job) }
          @job.reload.failed_at.should == nil
        end
      end
    end
  end
end

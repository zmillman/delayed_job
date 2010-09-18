require 'timeout'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/class/attribute_accessors'
require 'active_support/core_ext/kernel'
require 'logger'

module Delayed
  class Worker
    cattr_accessor :min_priority, :max_priority, :max_attempts, :max_run_time, :default_priority, :sleep_delay
    self.sleep_delay = 5
    self.max_attempts = 25
    self.max_run_time = 4.hours
    self.default_priority = 0

    # By default failed jobs are destroyed after too many attempts. If you want to keep them around
    # (perhaps to inspect the reason for the failure), set this to false.
    cattr_accessor :destroy_failed_jobs
    self.destroy_failed_jobs = true

    def self.logger=(*)
      warn "[DEPRECATED] Delayed::Worker.logger= no longer exists. Use the :log option with #initialize"
    end

    cattr_reader :backend
    attr_reader :worker_id, :options

    def self.backend=(backend)
      if backend.is_a? Symbol
        require "delayed/backend/#{backend}"
        backend = "Delayed::Backend::#{backend.to_s.classify}::Job".constantize
      end
      @@backend = backend
      silence_warnings { ::Delayed.const_set(:Job, backend) }
    end

    def self.guess_backend
      self.backend ||= :active_record if defined?(ActiveRecord)
    end

    def initialize(worker_id, options={})
      @worker_id = worker_id
      @options = options
      self.class.min_priority = options[:min_priority] if options.has_key?(:min_priority)
      self.class.max_priority = options[:max_priority] if options.has_key?(:max_priority)
    end

    def name
      @name ||= "host:#{Socket.gethostname rescue 'unknown'} pid:#{$$} worker:#{@worker_id}"
    end

    def name=(val)
      warn "[DEPRECATED] worker name can no longer be set"
    end

    def run(job)
      runtime =  Benchmark.realtime do
        Timeout.timeout(self.class.max_run_time.to_i) { job.invoke_job }
        job.destroy
      end
      logger.info "#{job.name} completed after %.4f" % runtime
      return true  # did work
    rescue Exception => e
      handle_failed_job(job, e)
      return false  # work failed
    end

    # Reschedule the job in the future (when a job fails).
    # Uses an exponential scale depending on the number of failed attempts.
    def reschedule(job, time = nil)
      if (job.attempts += 1) < self.class.max_attempts
        time ||= Job.db_time_now + (job.attempts ** 4) + 5
        job.run_at = time
        job.unlock
        job.save!
      else
        logger.info "PERMANENTLY removing #{job.name} because of #{job.attempts} consecutive failures."
        if job.respond_to?(:on_permanent_failure)
          warn "[DEPRECATION] The #on_permanent_failure hook has been renamed to #failure."
        end
        job.hook(:failure)
        self.class.destroy_failed_jobs ? job.destroy : job.update_attributes(:failed_at => Delayed::Job.db_time_now)
      end
    end

    def handle_failed_job(job, error)
      job.last_error = error.message + "\n" + error.backtrace.join("\n")
      logger.error "#{job.name} failed with #{error.class.name}: #{error.message} - #{job.attempts} failed attempts"
      reschedule(job)
    end

  private

    def logger
      @logger ||= Logger.new(options[:log] || 'log/delayed_job.log').tap do |logger|
        logger.formatter = Delayed::LoggerFormatter.new("Worker(#{name})")
      end
    end

  end
end

module Delayed
  class Master
    attr_reader :options, :children, :available_workers

    def initialize(options = {})
      @options = options
      @children = {}
      @available_workers = []
    end

    def start
      abort "Process is already running with pid #{pid}" if running?
      if pid_file.file?
        logger.info "Deleting stale pid file at #{pid_file}"
        pid_file.delete
      end

      GC.copy_on_write_friendly = true if GC.respond_to?(:copy_on_write_friendly=)

      require 'config/environment'

      Delayed::Job.before_fork

      @pid = fork { run }

      # Wait for process to finish starting
      sleep 0.1 until running? && pid_file.file?

      # detach the master process and exit;
      # note that Process#detach calls setsid(2)
      Process.detach @pid
      @pid
    end

    def run
      $0 = 'delayed_job'

      [:TERM, :INT, :QUIT].each do |sig|
        trap sig do
          logger.info "SIG#{sig} received. Shutting down."

          # reset trap handlers so we don't get caught in a trap loop
          trap :CLD, 'DEFAULT'
          trap sig, 'DEFAULT'

          # kill the children and reap them before terminating
          unless children.keys.empty?
            logger.debug "Terminating workers: #{children.inspect}"
            Process.kill :TERM, *children.keys
          end
          Process.waitall
          logger.info "Terminated workers"

          pid_file.delete if pid_file.file?

          # propagate the signal like a proper process should
          Process.kill sig, $$
          Process.waitall
        end
      end

      # # silence output like a proper daemon
      [STDIN, STDOUT, STDERR].each { |io| io.reopen '/dev/null' }

      Delayed::Job.after_fork

      # Spawn a new worker when one dies
      trap :CLD do
        handle_child_death
      end

      # Write pid file
      pid_file.dirname.mkpath
      pid_file.open('w') { |f| f.write $$ }

      logger.info "Starting with #{worker_count} workers"

      # Create the worker ids
      worker_count.times {|id| available_workers << id }

      loop do
        logger.debug "available_workers: #{available_workers.inspect}"
        logger.debug "busy workers: #{children.values.inspect}"
        while id = available_workers.shift
          sleep 5 if !spawn_worker(id)
        end
        logger.debug "No workers available, waiting for child death"
        handle_child_death
      end
    end

    def handle_child_death
      id = children.delete(Process.wait)
      # available_workers << children.delete(Process.wait)
      logger.debug "Worker #{id} reaped. status:#{$?.exitstatus}"
      spawn_worker id
    end

    def spawn_worker(id)
      worker = Worker.new(id)
      job = Delayed::Job.reserve(worker.name)
      if job
        logger.debug "Reserved job #{job.id} for worker #{id}"
        Delayed::Job.before_fork
        pid = fork do
          # $0 = "delayed_worker.#{id}"

          # reset all inherited traps from main process
          [:CLD, :HUP, :TERM, :INT, :QUIT].each { |sig| trap sig, 'DEFAULT' }
          Delayed::Job.after_fork
          worker.run(job)
        end
        children[pid] = id
        logger.debug "Forked worker #{id}. pid:#{pid}"
        pid
      else
        logger.debug "No jobs available for worker #{id}"
        available_workers << id
        false
      end
    end

    def worker_count
      options[:workers] ? options[:workers].to_i : 1
    end

    def stop
      abort "delayed_job is not running" unless running?
      Process.kill :TERM, pid

      # Wait for process to stop
      sleep 0.1 while running?
    end

    def root
      @root ||= Pathname.new(options[:root] || Dir.pwd)
    end

    def pid_file
      root.join('tmp/pids/delayed_job.pid')
    end

    def pid
      @pid ||= (pid_file.read.to_i if pid_file.file?)
    end

    def running?
      !!(pid && Process.getpgid(pid))
    rescue Errno::ESRCH
      false
    end

    def logger
      @logger ||= Logger.new(options[:log] || 'log/delayed_job.log').tap do |logger|
        logger.formatter = Delayed::LoggerFormatter.new("Master(#{pid})")
      end
    end
  end
end

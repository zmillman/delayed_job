module Delayed
  class Master
    attr_reader :options, :children, :available_workers

    def initialize(options = {})
      @options = options
      @children = {}
      @available_workers = []
    end

    def start
      if GC.respond_to?(:copy_on_write_friendly=)
        GC.copy_on_write_friendly = true
      end

      abort "Process is already running with pid #{pid}" if running?
      if pid_file.file?
        warn "Deleting stale pid file at #{pid_file}"
        pid_file.delete
      end

      require 'config/environment'

      Delayed::Job.before_fork

      @pid = fork do
        $0 = 'delayed_job'

        [:TERM, :INT, :QUIT].each do |sig|
          trap sig do
            logger.info "SIG#{sig} received. Shutting down."

            # # kill the children and reap them before terminating
            # Process.kill :TERM, *children.keys
            # Process.waitall

            pid_file.delete if pid_file.file?

            # reset trap handlers so we don't get caught in a trap loop
            trap sig, 'DEFAULT'

            # propagate the signal like a proper process should
            Process.kill sig, $$

            # FIXME: I don't understand why, but process will not stop without following
            Process.wait
          end
        end

        # Write pid file
        pid_file.dirname.mkpath
        pid_file.open('w') { |f| f.write $$ }

        logger.info "Starting with #{worker_count} workers"

        # # silence output like a proper daemon
        # [$stdin, $stdout, $stderr].each { |io| io.reopen '/dev/null' }

        Delayed::Job.after_fork

        run
      end

      # Wait for process to finish starting
      sleep 0.1 until running? && pid_file.file?

      # # detach the master process and exit;
      # # note that Process#detach calls setsid(2)
      # Process.detach pid
      pid
    end

    def run
      # Spawn a new worker when one dies
      trap :CLD do
        id = children.delete Process.wait
        spawn_worker(id)
      end

      # Create the worker ids
      worker_count.times {|id| available_workers << id }

      loop do
        available_workers.each do |id|
          if !spawn_worker(id)
            sleep 5
          end
        end
      end
    end

    def spawn_worker(id)
      logger.debug "Spawning worker #{id}"
      worker = Worker.new(id)
      job = Delayed::Job.reserve(worker.name)
      if job
        available_workers.delete(id)

        Delayed::Job.before_fork
        pid = fork do
          # $0 = "delayed_worker.#{id}"
          #
          # # reset all inherited traps from main process
          # [:CLD, :HUP, :TERM, :INT, :QUIT].each { |sig| trap sig, 'DEFAULT' }
          Delayed::Job.after_fork
          worker.run(job)
        end

        children[pid] = id
        pid
      else
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

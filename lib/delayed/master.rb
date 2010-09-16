module Delayed
  class Master
    def initialize(options = {})
      @options = options
    end

    def start
      # # Ensure new file permissions are set to a standard 0755
      # File.umask 0022

      abort "Process is already running with pid #{pid}" if running?

      @pid = fork do
        $0 = 'delayed_job'

        [:TERM, :INT, :QUIT].each do |sig|
          trap sig do
            # logger.info "SIG#{sig} received! Shutting down workers."

            # reset trap handlers so we don't get caught in a trap loop
            trap sig, 'DEFAULT'

            # # kill the children and reap them before terminating
            # Process.kill :TERM, *children.keys
            # Process.waitall

            pid_file.delete if pid_file.file?

            # propagate the signal like a proper process should
            Process.kill sig, $$
          end
        end

        # Write pid file
        pid_file.dirname.mkpath
        pid_file.open('w') { |f| f.write $$ }

        # # silence output like a proper daemon
        # [$stdin, $stdout, $stderr].each { |io| io.reopen '/dev/null' }

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
      # # spawn the first workers
      # children, times_dead = {}, {}
      # worker_count = (ENV['WORKERS'] || 1).to_i
      # worker_count.times { |id| children[worker.call id, nil] = id }
      #
      # # and respawn the failures
      # trap :CLD do
      #   id = children.delete Process.wait
      #
      #   # check to see if this worker is dying repeatedly
      #   times_dead[id] ||= []
      #   times_dead[id] << (now = Time.now)
      #   times_dead[id].reject! { |time| now - time > 60 }
      #   if times_dead[id].size > 4
      #     delay = 60 * 5 # time to tell the children to sleep before loading
      #   else
      #     rails_logger.call "Restarting dead worker: delayed_worker.#{id}"
      #   end
      #
      #   children[worker.call id, delay] = id
      # end
      #
      # # restart children on SIGHUP
      # trap :HUP do
      #   rails_logger.call 'SIGHUP received! Restarting workers.'
      #   Process.kill :TERM, *children.keys
      # end
      #
      # terminate children on user termination

      # replace with real work
      loop { sleep 5 }
    end

    def stop
      abort "delayed_job is not running" unless running?
      Process.kill :TERM, pid

      # Wait for process to stop
      sleep 0.1 while running?
    end

    def root
      @root ||= Pathname.new(@options[:root] || Dir.pwd)
    end

    def pid_file
      root.join('tmp/pids/delayed_job.pid')
    end

    def pid
      @pid ||= pid_file.read.to_i if pid_file.file?
    end

    def running?
      !!(pid && Process.getpgid(pid))
    rescue Errno::ESRCH
      false
    end
  end

  class Worker
    # # Loads the Rails environment and spawns a worker
    # worker = lambda do |id, delay|
    #   fork do
    #     $0 = "delayed_worker.#{id}"
    #
    #     # reset all inherited traps from main process
    #     [:CLD, :HUP, :TERM, :INT, :QUIT].each { |sig| trap sig, 'DEFAULT' }
    #
    #     # lay quiet for a while before booting up if specified
    #     sleep delay if delay
    #
    #     # Boot the rails environment and start a worker
    #     Rake::Task[:environment].invoke
    #     Delayed::Worker.logger = Logger.new logfile
    #     Delayed::Worker.new({
    #       :min_priority => ENV['MIN_PRIORITY'],
    #       :max_priority => ENV['MAX_PRIORITY'],
    #       :quiet => true
    #     }).start
    #   end
    # end

  end

end
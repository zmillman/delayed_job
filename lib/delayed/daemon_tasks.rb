# The command
#
#     $ WORKERS=n RAILS_ENV=production rake jobs:daemon:start
#
# spawns a simple forking daemon, which spawns and restarts `n' instances of
# Delayed::Worker. Worker processes are revived by the master process on receipt
# of SIGCLD.
#
# We can restart worker processes by sending SIGHUP to the master process or by
# killing them directly. Sending SIGTERM, SIGINT, or SIGQUIT to the master
# instructs it to kill its children and terminate.
#
# Alternately, there are the tasks `jobs:daemon:restart' and `jobs:daemon:stop'
#
# Two extra features:
#
# * To avoid CPU thrashing, if a child worker dies 4 times in 60 seconds, a
#   warning message is logged and the child sleeps for 300 seconds before
#   booting up
#
# * The master polls tmp/restart.txt and restarts children on timestamp update

namespace :jobs do
  namespace :daemon do
    desc 'Spawn a daemon which forks WORKERS=n instances of Delayed::Worker'
    task :start do
      # we want master and children to share a logfile, so set these before fork
      rails_env  = ENV['RAILS_ENV'] || 'development'
      rails_root = Dir.pwd  # rake sets cwd to the parent dir of the Rakefile
      logfile    = "#{rails_root}/log/delayed_worker.#{rails_env}.log"

      # Ensure new file permissions are set to a standard 0755
      File.umask 0022

      # Loads the Rails environment and spawns a worker
      worker = lambda do |id, delay|
        fork do
          $0 = "delayed_worker.#{id}"

          # reset all inherited traps from main process
          [:CLD, :HUP, :TERM, :INT, :QUIT].each { |sig| trap sig, 'DEFAULT' }

          # lay quiet for a while before booting up if specified
          sleep delay if delay

          # Boot the rails environment and start a worker
          Rake::Task[:environment].invoke
          Delayed::Worker.logger = Logger.new logfile
          Delayed::Worker.new({
            :min_priority => ENV['MIN_PRIORITY'],
            :max_priority => ENV['MAX_PRIORITY'],
            :quiet => true
          }).start
        end
      end

      # fork a simple master process
      master = fork do
        $0 = 'delayed_worker.master'

        # simple logger; there is some overhead due to reopening the file for
        # every write, but it's minor, and avoids headaches with open files
        rails_logger = lambda do |msg|
          File.open logfile, 'a' do |f|
            f.puts "#{Time.now.strftime '%FT%T%z'}: [#{$0}] #{msg}"
          end
        end

        # create pidfile or abort
        pid_dir  = "#{rails_root}/tmp/pids"
        pid_file = "#{pid_dir}/#{$0}.pid"
        if File.exists? pid_file
          msg = "PID file #{pid_file} already exists!"
          rails_logger.call msg
          abort msg
        else
          # silence output like a proper daemon
          [$stdin, $stdout, $stderr].each { |io| io.reopen '/dev/null' }
          mkdir_p pid_dir, :verbose => false
          File.open(pid_file, 'w') { |f| f.write $$ }
        end

        # spawn the first workers
        children, times_dead = {}, {}
        worker_count = (ENV['WORKERS'] || 1).to_i
        rails_logger.call "Spawning #{worker_count} worker(s)"
        worker_count.times { |id| children[worker.call id, nil] = id }

        # and respawn the failures
        trap :CLD do
          id = children.delete Process.wait

          # check to see if this worker is dying repeatedly
          times_dead[id] ||= []
          times_dead[id] << (now = Time.now)
          times_dead[id].reject! { |time| now - time > 60 }
          if times_dead[id].size > 4
            delay = 60 * 5 # time to tell the children to sleep before loading
            rails_logger.call %Q{
              delayed_worker.#{id} has died four times in the past minute!
              Something is seriously wrong!
              Restarting worker in #{delay} seconds.
            }.strip.gsub /\s+/, ' '
          else
            rails_logger.call "Restarting dead worker: delayed_worker.#{id}"
          end

          children[worker.call id, delay] = id
        end

        # restart children on SIGHUP
        trap :HUP do
          rails_logger.call 'SIGHUP received! Restarting workers.'
          Process.kill :TERM, *children.keys
        end

        # terminate children on user termination
        [:TERM, :INT, :QUIT].each do |sig|
          trap sig do
            rails_logger.call "SIG#{sig} received! Shutting down workers."

            # reset trap handlers so we don't get caught in a trap loop
            [:CLD, sig].each { |s| trap s, 'DEFAULT' }

            # kill the children and reap them before terminating
            Process.kill :TERM, *children.keys
            Process.waitall
            rm_f pid_file

            # propagate the signal like a proper process should
            Process.kill sig, $$
          end
        end

        # NOTE: We want to block on something so that Process.waitall doesn't
        #       reap dead children before the SIGCLD handler does
        #
        # poll passenger restart file and restart on update
        years_ago = lambda { |n| Time.now - 60 * 60 * 24 * 365 * n }
        mtime = lambda do |file|
          File.exists?(file) ? File.mtime(file) : years_ago.call(2)
        end
        restart_file  = "#{rails_root}/tmp/restart.txt"
        last_modified = mtime.call restart_file
        loop do
          if (check = mtime.call restart_file) > last_modified
            last_modified = check
            Process.kill :HUP, $$
          end
          sleep 5
        end

        # reap children and remove logfile if the blocking loop is broken
        Process.waitall
        rm_f pid_file
      end

      # detach the master process and exit;
      # note that Process#detach calls setsid(2)
      Process.detach master
    end

    [%w[restart SIGHUP], %w[stop SIGTERM]].each do |name, signal|
      instance_eval do
        desc "#{name.capitalize} an existing delayed_worker daemon"
        task name do
          begin
            pid_file = "#{Dir.pwd}/tmp/pids/delayed_worker.master.pid"
            abort 'No pid file found!' unless File.exists? pid_file
            pid = File.read(pid_file).to_i
            puts "Sending #{signal} to #{pid}"
            Process.kill signal, pid
          rescue Errno::ESRCH => e
            abort e.to_s
          end
        end
      end
    end
  end
end

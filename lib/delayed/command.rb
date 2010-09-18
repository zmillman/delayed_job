require 'rubygems'
require 'optparse'
require 'delayed/master'

module Delayed
  class Command
    attr_reader :options, :master

    COMMANDS = %w(start stop restart run)

    def initialize(args)
      @options = {
        :workers => 1
      }

      @parser = OptionParser.new do |opts|
        opts.banner = "Usage: #{File.basename($0)} [options] start|stop|restart|run"

        opts.on('-h', '--help', 'Show this message') do
          puts opts
          exit 1
        end
        opts.on('-e', '--environment=NAME', 'Specifies the environment to run this delayed jobs under (test/development/production).') do |e|
          ENV['RAILS_ENV'] = e
        end
        opts.on('--min-priority N', 'Minimum priority of jobs to run.') do |n|
          @options[:min_priority] = n
        end
        opts.on('--max-priority N', 'Maximum priority of jobs to run.') do |n|
          @options[:max_priority] = n
        end
        opts.on('-n', '--workers=N', "Number of unique workers to spawn") do |workers|
          @options[:workers] = workers.to_i
        end
        opts.on('--number_of_workers=workers', "DEPRECATED") do |workers|
          abort "--number_of_workers is deprecated. Use --workers"
        end
        opts.on('--pid-dir=DIR', 'Specifies an alternate directory in which to store the process ids.') do |dir|
          @options[:pid_dir] = dir
        end
      end
      @args = @parser.parse!(args)
      @command = args.shift
      @master = Delayed::Master.new(@options)
    end

    def run
      if COMMANDS.include?(@command)
        @master.send(@command)
      else
        STDERR.puts @parser
        error = @command ? "Unknown command: #{@command}" : "Command required"
        abort "\n#{error}. Use one of #{COMMANDS.join(', ')}"
      end
    end
  end
end

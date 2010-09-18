require 'spec_helper'

describe Delayed::Command do
  def command(*args)
    Delayed::Command.new(args)
  end

  describe "run" do
    %w(start stop restart run).each do |action|
      it "should call #{action} on master" do
        cmd = command(action)
        cmd.master.should_receive(action)
        cmd.run
      end
    end

    it "should abort when calling unknown command" do
      silence_stderr { Process.wait(fork { command('foo').run }) }
      $?.exitstatus.should == 1
    end
  end

  describe "options" do
    describe "help" do
      %w(-h --help).each do |option|
        it "should exit with status of 1 with #{option}" do
          silence_stream(STDOUT) { Process.wait(fork { command(option) }) }
          $?.exitstatus.should == 1
        end
      end
    end

    describe "workers" do
      it "should default to 1" do
        command.options[:workers].should == 1
      end

      it "should parse -n" do
        command('-n', '3').options[:workers].should == 3
      end
    end

    describe "environment" do
      after { ENV['RAILS_ENV'] = 'test' }

      [['-e', 'production'], ['--environment=production']].each do |argv|
        it "should set RAILS_ENV" do
          command(*argv)
          ENV['RAILS_ENV'].should == 'production'
        end
      end
    end
  end
end
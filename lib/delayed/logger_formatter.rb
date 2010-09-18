module Delayed
  class LoggerFormatter < Logger::SimpleFormatter
    def initialize(prefix)
      @prefix = prefix
    end

    def call(severity, timestamp, progname, msg)
      @prefix + " - " + super
    end
  end
end
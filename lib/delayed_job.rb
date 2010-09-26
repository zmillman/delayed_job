require 'active_support'

require File.dirname(__FILE__) + '/delayed/message_sending'
require File.dirname(__FILE__) + '/delayed/performable_method'
require File.dirname(__FILE__) + '/delayed/performable_mailer' if defined?(ActionMailer)
require File.dirname(__FILE__) + '/delayed/yaml_ext'
require File.dirname(__FILE__) + '/delayed/backend/base'
require File.dirname(__FILE__) + '/delayed/worker'
require File.dirname(__FILE__) + '/delayed/railtie' if defined?(Rails::Railtie)
require 'delayed/logger_formatter'

module Delayed
  autoload :Command,            'delayed/command'
  autoload :Master,             'delayed/master'
end

Object.send(:include, Delayed::MessageSending)
Module.send(:include, Delayed::MessageSending::ClassMethods)

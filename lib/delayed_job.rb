require 'active_support'
require 'delayed/message_sending'
require 'delayed/performable_method'

# PerformableMailer is compatible with ActionMailer 3 (and possibly 3.1)
if defined?(ActionMailer)
  require 'action_mailer/version'
  require 'delayed/performable_mailer' if 3 == ActionMailer::VERSION::MAJOR
end

# The yaml extensions will be loaded by the DJ railtie when using Rails. This is to prevent early
# access to Active Record which triggers a db connection
# https://github.com/collectiveidea/delayed_job/issues/405
require 'delayed/yaml_ext' unless defined?(Rails::Railtie)

require 'delayed/lifecycle'
require 'delayed/plugin'
require 'delayed/plugins/clear_locks'
require 'delayed/backend/base'
require 'delayed/worker'
require 'delayed/deserialization_error'
require 'delayed/railtie' if defined?(Rails::Railtie)

Object.send(:include, Delayed::MessageSending)
Module.send(:include, Delayed::MessageSending::ClassMethods)

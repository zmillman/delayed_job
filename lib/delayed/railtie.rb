require 'delayed_job'
require 'rails'

module Delayed
  class Railtie < Rails::Railtie
    initializer :after_initialize do
      ActiveSupport.on_load(:action_mailer) do
        ActionMailer::Base.send(:extend, Delayed::DelayMail)
      end

      ActiveSupport.on_load(:active_record) do
        require 'delayed/yaml_ext'
      end
    end

    rake_tasks do
      load 'delayed/tasks.rb'
    end
  end
end

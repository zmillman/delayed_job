$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'rubygems'
require 'bundler/setup'
require 'rspec'
require 'logger'

require 'active_record'
require 'action_mailer'

require 'delayed_job'
require 'delayed/backend/shared_spec'

Delayed::Worker.logger = Logger.new('/tmp/dj.log')
ENV['RAILS_ENV'] = 'test'
require 'rails'

config = YAML.load(File.read('spec/database.yml'))
ActiveRecord::Base.configurations = {'test' => config['mysql']}
ActiveRecord::Base.establish_connection
ActiveRecord::Base.logger = Delayed::Worker.logger
ActiveRecord::Migration.verbose = false

ActiveRecord::Schema.define do
  create_table :delayed_jobs, :force => true do |table|
    table.string   :queue
    table.integer  :attempts, :default => 0
    table.text     :handler
    table.text     :last_error
    table.datetime :run_at
    table.datetime :locked_at
    table.datetime :failed_at
    table.string   :locked_by
    table.timestamps
  end

  add_index :delayed_jobs, [:queue, :run_at], :name => 'delayed_jobs_queue'

  create_table :stories, :force => true do |table|
    table.string :text
  end
end

# Purely useful for test cases...
class Story < ActiveRecord::Base
  def tell; text; end
  def whatever(n, _); tell*n; end

  handle_asynchronously :whatever
end

Delayed::Worker.backend = :active_record

# Add this directory so the ActiveSupport autoloading works
ActiveSupport::Dependencies.autoload_paths << File.dirname(__FILE__)

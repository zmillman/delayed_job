class CreateDelayedJobs < ActiveRecord::Migration
  def self.up
    create_table :delayed_jobs, :force => true do |table|
      table.string   :queue                        # put jobs in different queues to give them more importantce
      table.integer  :attempts, :default => 0      # Provides for retries, but still fail eventually.
      table.text     :handler                      # YAML-encoded string of the object that will do work
      table.text     :last_error                   # reason for last failure (See Note below)
      table.datetime :run_at                       # When to run. Could be Time.zone.now for immediately, or sometime in the future.
      table.datetime :locked_at                    # Set when a client is working on this object
      table.datetime :failed_at                    # Set when all retries have failed (actually, by default, the record is deleted instead)
      table.string   :locked_by                    # Who is working on this object (if locked)
      table.timestamps
    end

    add_index :delayed_jobs, [:queue, :run_at], :name => 'delayed_jobs_queue'
  end

  def self.down
    drop_table :delayed_jobs
  end
end
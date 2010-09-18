require 'spec/spec_helper'
require 'logger'
require 'benchmark'

Benchmark.bm(10) do |x|
  Delayed::Job.delete_all
  n = 10000
  n.times { "foo".delay.length }

  master = Delayed::Master.new(:workers => 20)
  master.stop if master.running?

  x.report do
    master.start
    sleep 1 until Delayed::Job.count == 0
  end

  master.stop
end

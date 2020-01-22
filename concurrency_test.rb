require 'concurrent-edge'

class Computer < Concurrent::Actor::RestartingContext
  def initialize
    super()
    @jobs = {}
  end

  def on_message(msg)
    command, *args = msg
    case command
    # new job to process
    when :run
      job        = args[0]
      @jobs[job] = envelope.future
      # Process asynchronously and send message back when done.
      Concurrent::Promises.future(&job).chain(job) do |fulfilled, value, reason, job|
        self.tell [:done, job, fulfilled, value, reason]
      end
      # Do not make return value of this method to be answer of this message.
      # We are answering later in :done by resolving the future kept in @jobs.
      Concurrent::Actor::Behaviour::MESSAGE_PROCESSED
    when :done
      job, fulfilled, value, reason = *args
      future                        = @jobs.delete job
      # Answer the job's result.
      future.resolve fulfilled, value, reason
    when :status
      { running_jobs: @jobs.size }
    else
      # Continue to fail with unknown message.
      pass 
    end
  end
end

computer = Concurrent::Actor.spawn Computer, :computer
puts "1"
# => #<Concurrent::Actor::Reference:0x00002e /computer (Computer)>
results = 300000.times.map { computer.ask [:run, -> { sleep 3; :result }] }
# => [#<Concurrent::Promises::Future:0x00002f pending>,
#     #<Concurrent::Promises::Future:0x000030 pending>,
#     #<Concurrent::Promises::Future:0x000031 pending>]
puts "2"
puts computer.ask(:status).value!             # => {:running_jobs=>3}
puts "3"
results.map(&:value!)                    # => [:result, :result, :result]
puts "4"

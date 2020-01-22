# coding: utf-8
# frozen_string_literal: true

require 'clamp'
require 'byebug'
require 'fileutils'
require 'ruby-progressbar'
require 'concurrent-edge'
require 'connection_pool'
require 'mysql2'
require './header_parser.rb'
require 'json'
require 'mail'
require 'csv'
require 'RubySpamAssassin'
include RubySpamAssassin

class Computer < Concurrent::Actor::RestartingContext
  def initialize
    super()
    @jobs = {}
    @finished_count = 0
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
        # byebug unless reason.empty?
        self.tell [:done, job, fulfilled, value, reason]
      end
      # Do not make return value of this method to be answer of this message.
      # We are answering later in :done by resolving the future kept in @jobs.
      Concurrent::Actor::Behaviour::MESSAGE_PROCESSED
    when :done
      job, fulfilled, value, reason = *args
      future                        = @jobs.delete job
      # Answer the job's result.
      # byebug unless value.empty?
      @finished_count += value[:total]

      future.resolve fulfilled, value, reason
      future = nil
    when :status
      status = { running_jobs: @jobs.size, finished_count: @finished_count }
      @finished_count = 0
      status
    else
      # Continue to fail with unknown message.
      pass
    end
  end
end                                      # => :on_message

Clamp do
  option ['-s', '--size'],
         'SIZE',
         'the number of lines to processes concurrently',
         default: 5,
         attribute_name: :size do |s|
    Integer(s)
  end

  option ['--mysql-timeout'],
         'MYSQL_TIMEOUT',
         'the timeout for any mysql transaction',
         default: 5,
         attribute_name: :mysql_timeout do |t|
           Integer(t)
         end

  option ['-u', '--user'],
         'USER',
         'username for mysql local database',
         attribute_name: :mysql_user

  option ['-p', '--password'],
         'PASSWORD',
         'password for mysql local database',
         default: nil,
         attribute_name: :mysql_password

  option ['-h', '--host'],
         'HOST',
         'host for mysql server',
         default: 'localhost',
         attribute_name: :mysql_host

  option ['-v', '--verbose'],
         :flag,
         'be talky'

  option ['-e', '--eml'],
         'EML OUTPUT DIRECTORY',
         'output directory for eml files, no out put if not set',
         default: nil,
         attribute_name: :eml_output_directory

  option ['-o', '--offset'],
         'DB OFFSET',
         'offset to start from when getting rows',
         default: 0,
         attribute_name: :offset do |o|
    Integer(o)
  end

  option ['-i', '--id'],
         'START ID',
         'id to start with',
         default: 0,
         attribute_name: :start_id

  option ['--debug'],
         :flag,
         'debug mode, no threading, verbose'

  parameter 'DATABASE', 'the database to convert everything into', attribute_name: :database

  def execute
    puts "Processing database #{database} ⚙️  ⚙️= ⚙️ "

    @verbose = verbose?
    @size = size
    @mysql_timeout = mysql_timeout
    @database = database
    @eml_output_directory = eml_output_directory
    @mysql_host = mysql_host
    @mysql_password = mysql_password
    @offset = offset
    @cores = Concurrent.processor_count - 1
    @last_id = start_id

    if debug?
      @debug = debug?
      @verbose = true
      @cores = 1
    end

    # Just to keep one in the queue
    # @cores += 1

    # Steps
    # Start up a connection pool
    mysql_connection_pool = start_mysql_connection_pool @cores * 5,
                                                        mysql_timeout,
                                                        mysql_user,
                                                        mysql_password,
                                                        database,
                                                        mysql_host

    mysql_count = mysql_connection_pool.with do |mysql_client|
      get_row_count(mysql_client)
    end

    puts "Found about #{mysql_count - offset} records to process"
    puts "Processing on #{@cores} cores"
    puts 'Starting processing 🏁 🏁 🏁'

    @progressbar = ProgressBar.create total: mysql_count - offset,
                                      throttle_rate: 0.5,
                                      format: '%t: |%B| : %c/%C : %p%% : %a : %E '

    @error_count = 0
    start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    # Get X number of lines from the database

    computer = Concurrent::Actor.spawn Computer, :computer

    client = new_mysql_client mysql_user,
                              mysql_password,
                              database,
                              mysql_host
    @finished_count = 0 if @debug
    loop do
    #1000.times do
      #futures = [] if futures.nil?
      #OK, let's start this over.
      #We create a pool of futures of a certain size
      #We then load up futures, grabbing, call it 200 mysql rows for each one until we hit a size
      #When one finishes, we grab the next, etc.
      unless @debug
        status = computer.ask(:status).value!
        running_jobs = status[:running_jobs]
        @progressbar.progress += status[:finished_count]
        @progressbar.title = "#{running_jobs} jobs running, last_id #{@last_id}"
      else
        running_jobs = 0
      end

      if running_jobs < @size
        mysql_results = get_database_results client, (@cores * @size) - (running_jobs * @size), @last_id
        #break if mysql_results.count < @size.to_i

        break if mysql_results.count.zero? && running_jobs.zero?
        @last_id = mysql_results.to_a.last["id"] unless mysql_results.count.zero?
        @offset += mysql_results.count

#        mysql_results.each do |result|
#          futures << create_future(result, mysql_connection_pool)
#          @progressbar.increment
#      end
        # mysql_results
        mysql_results.each_slice(@size) do |results|
          @progressbar.log @last_id

          if @debug
            begin
              process_mysql_result results, mysql_user, mysql_password, database, mysql_host
              @finished_count += results.count
              @progressbar.progress += results.count
              @progressbar.title = "Debug Mode: last_id #{@last_id}"
            rescue Exception => e
              @progressbar.log "Error processing slice: #{e}"
              @progressbar.log "Backtrack: #{e.backtrace}"
            end
          else
            # byebug
            computer.ask [:run, -> { process_mysql_result results, mysql_user, mysql_password, database, mysql_host }]
          end
          # @progressbar.progress += @size
        end
        mysql_results = nil
      end
    end

    @progressbar.finish
    end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    elapsed_time = end_time - start_time

    puts 'Finished! 🏁 🙌 🎉'
    puts "Total time: #{elapsed_time}"
    puts "Number of records: #{mysql_count}"
    puts "Number of errors: #{@error_count}"
  end

  def start_mysql_connection_pool(size, timeout, user, password, database, host)
    ConnectionPool.new(size: size, timeout: @mysql_timeout) do
      new_mysql_client(user, password, database, host)
    end
  end

  def new_mysql_client(user, password, database, host)
    Mysql2::Client.new(host: host,
                       database: database,
                       username: user,
                       password: password,
                       cache_rows: false
                      )
  end

  def get_row_count(client)
    results = client.query("SELECT TABLE_NAME AS 'User Emails', TABLE_ROWS AS 'Rows' FROM information_schema.TABLES WHERE TABLES.TABLE_SCHEMA = '#{@database}' AND TABLES.TABLE_TYPE = 'BASE TABLE';")
    results.first["Rows"]
  end

  def get_database_results(client, count, id)
    client.query("SELECT * FROM user_emails WHERE id > #{id} ORDER BY id LIMIT #{count}", cache_rows: false)
  end

  def process_mysql_result(mysql_results, mysql_user, mysql_password, database, mysql_host)
      mysql_conn = new_mysql_client mysql_user,
                                    mysql_password,
                                    database,
                                    mysql_host
      errors = []
      mysql_results.each do |mysql_result|
        begin
          headers = parse_headers mysql_result
          id = headers[:id]
          results = headers[:parsed_results]

          # Add to the database
          to = mysql_conn.escape results['to'] unless results['to'].nil?
          from = mysql_conn.escape results['from'] unless results['from'].nil?

          results_as_json = results.to_json
          json_headers = mysql_conn.escape results_as_json
          results_as_json = nil

          email_format = eml_format(mysql_result, headers)
          is_spam = check_if_spam(headers, email_format)
          # byebug if is_spam == false
          mysql_conn.query("UPDATE `user_emails` SET `to` = '#{to}', `from` = '#{from}', `json` = '#{json_headers}' WHERE `id` = #{id};")

          next if is_spam == true

          unless @eml_output_directory.nil?
            File.open "#{@eml_output_directory}/#{mysql_result['id']}.eml", 'w' do |f|
              f.write email_format
            end
          end
        rescue Exception => e
          @progressbar.log "Error processing slice: #{e}"
          @progressbar.log "Backtrace: #{e.backtrace}"
          byebug
          # errors << mysql_result["id"]
        end
      end

      mysql_conn.close
      mysql_conn = nil

      { total: mysql_results.count }
  end

  def check_if_spam(headers, eml)
    # if 'eml' is nil, we don't even bother, kill it
    return true if eml.nil?

    # First, we check the headers for a 'x-spam-status', and parse that
    # if possible. If not, we then send it through to SpamAssassin
    spam_status_raw = headers[:parsed_results]['x-spam-status']

    if spam_status_raw.nil?
      # spam_client = SpamClient.new("0.0.0.0", "783", 20)
      # report = spam_client.check(eml)
      # spam_status = report.score
      # @progressbar.log "Found novel spam score of: #{spam_status}"
      # For the moment it seems as if these are not spam in general
      return false
    else
      spam_status = parse_header_spam_status(spam_status_raw)
      # @progressbar.log "Found reported spam score of: #{spam_status}"
    end

    return false if spam_status.to_f < 2
    true
  end

  def parse_header_spam_status(spam_status)
    extracted_string = spam_status[14..-2]
    return nil if extracted_string.nil?
    extracted_string.to_f.abs
  end

  def eml_format(result, headers)
    encoding = clean_encoding result['encoding']

    begin
      mail = Mail.new do

        headers[:parsed_results].each do |key, value|
          header[key] = value
        end

        text_part do
          content_type encoding
          body result['plain_text']
        end

        html_part do
          content_type "text/html; charset=#{encoding}"
          body result['html']
        end

        charset = encoding
      end

      mail.to_s
    rescue Exception => e
      #byebug
    end
  end

  def clean_encoding encoding
    return "" if encoding.nil?

    index = encoding.index '$'
    encoding[0...index]
  end

  def parse_headers(mysql_result)
    # Parsing:
    # Pull out the headers
    # Parse headers
    # Get a connection from the pool
    # Insert back into database under the correct column

    # Temporarily!
    headers = mysql_result["headers"]
    parser = HeaderParser.new(headers)
    parsed_results = parser.parse
    parser = nil
    { id: mysql_result['id'], parsed_results: parsed_results }
  end
end

class RowProcessingError < StandardError
  def initialize(row, message)
    @row = row
    @message = message
  end

  def to_s
    "RowProcessingError: id: #{row['id']}, '#{@message}'"
  end
end

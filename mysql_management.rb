# coding: utf-8
# mysql -u chris -p formationshouse0 < x_0.sql

# frozen_string_literal: true

require 'clamp'
require 'byebug'
require 'fileutils'
require 'ruby-progressbar'

Clamp do
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
         attribute_name: :mysql_host,
         default: 'localhost'

  parameter 'FOLDER', 'the sql folder or file to split', attribute_name: :folder_name
  parameter 'DATABASE', 'the database to import everything into', attribute_name: :database
  
  def execute
    # Open file
    unless File.exist?(folder_name)
      puts "❌ Error: Folder or file #{folder_name} not found" && exit(1)
    end

    @mysql_user = mysql_user
    @mysql_password = mysql_password
    @database = database
    @mysql_host = mysql_host
    
    path = "#{folder_name}"
    path += '/*' if File.directory? folder_name
    
    files = Dir.glob("#{path}")
    files_count = files.count

    puts "Importing #{files_count} files... 🏁 🏁 🏁"

    @error_count = 0
    @progressbar = ProgressBar.create(starting_at: 0,
                                      total: files_count,
                                      throttle_rate: 0.1,
                                      format: '%t: |%B| : %c/%C : %p%% : %a : %E ')

    start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    import_files files

    @progressbar.finish
    end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    elapsed_time = end_time - start_time

    puts '"DONE!!!! 👾 🙌 🎉 🥂 🎈 🍾 🐝 🔥'
    puts "Total time: #{elapsed_time}"
    puts "Number of files: #{files_count}"
    puts "Number of errors: #{@error_count}"
  end

  def import_files(files)
    files.each do |file|
      import_file file
      @progressbar.increment
    end
  end

  def import_file(file)
    if @mysql_password.nil?
      sql_command = "mysql --host=#{@mysql_host} --max_allowed_packet=100M -u #{@mysql_user} #{database} < #{file}"
    else
      sql_command = "MYSQL_PWD=#{mysql_password} mysql --host=#{@mysql_host} --max_allowed_packet=100M -u #{@mysql_user} #{database} < #{file}"
    end

    system 'bash', '-c', sql_command

    unless $?.exitstatus.zero?
      puts "😔 Error processing #{file}"
      @error_count += 1
    end
  end
end

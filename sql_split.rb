# coding: utf-8
# frozen_string_literal: true

require 'clamp'
require 'byebug'
require 'fileutils'
require 'ruby-progressbar'

Clamp do
  option ['-l', '--lines'],
         :lines, 'number of inserts to put into the file', default: nil do |l|
    Integer(l)
  end

  option ['-s', '--size'],
         :size, 'size for each output to be as a max, in MB', default: nil do |s|
    Integer(s) * 1_000_000
  end

  option ['-a', '--archive'], :flag, 'for archives'

  parameter 'FILE', 'the sql file to split', attribute_name: :file_name

  def execute
    # Open file
    unless File.exist?(file_name)
      puts "❌ Error: File #{file_name} not found"
      exit(1)
    end

    file = File.open(file_name, 'r')
    @directory = "#{File.dirname(file_name)}/output"
    FileUtils.remove_dir(@directory, true)
    FileUtils.mkdir(@directory)

    header = get_file_header file, archive?
    puts '❌ Error: No sql INSERT commmands found' && exit(2) if header.nil?

    @progressbar = ProgressBar.create(starting_at: 0,
                                      total: nil,
                                      throttle_rate: 0.1)

    @total_files = 0
    start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    total = split_into_lines file, lines, header, size
    @progressbar.finish
    end_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    elapsed_time = end_time - start_time

    puts 'DONE!!!! 👾 🙌 🎉 🥂 🎈 🍾 🐝 🔥'
    puts "Total time: #{elapsed_time}"
    puts "Total lines processed: #{total}"
    puts "Number of files: #{@total_files}"
  end

  def get_file_header(file, archive)
    # First, let's get everything before the first "INSERT" command
    header = ''
    until file.eof?
      line = file.readline
      break if line.start_with? 'INSERT INTO'

      if archive == true && line.start_with?('/*!40000 ALTER TABLE')
        # header += 'LOCK TABLE `user_emails_archive` WRITE;\n'
        header += line
        header += "LOCK TABLES `user_emails` WRITE;\n"
      else      
        header += line
      end
    end
    return nil if file.eof?

    header
  end

  def split_into_lines(input_file, number_of_lines, header, size)
    line_count = 0
    file_count = 0
    total_lines = 0
    output_file = start_file(file_count, header)

    until input_file.eof?
      line = input_file.readline

      line.encode!('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
      matches = line.split(/(\([0-9]+,')/)
      
      # If there are no matches we assume something else is up,
      # so we just write the line
      if matches.count.zero?
        output_file.write line
        next
      end

      matches.delete_at(0)

      id_block = nil
      matches.each do |new_line|
        # Cut off the old file if we've reached the correct number of lines
        if (number_of_lines.nil? == false && (line_count += 1) > number_of_lines) ||
           (size.nil? == false && output_file.size > size)
          output_file = reset_file(output_file, file_count += 1, header)
          line_count = 0
        end

        if id_block.nil?
          id_block = new_line
          next
        end

        new_line = id_block + new_line
        id_block = nil

        unless new_line.start_with? 'INSERT INTO'
          prefix = 'INSERT INTO `user_emails` VALUES '
        end

        last_character = new_line[-1, 1]
        if last_character != ';'
          new_line.delete_suffix!(',')
          new_line += ';'
        end
        output_file.write prefix + new_line + "\n"
      end

      @progressbar.log "File Count: #{file_count}"
      total_lines += (matches.count / 2)
      @progressbar.increment
    end

    output_file.close

    total_lines
  end

  def start_file(count, header)
    @total_files += 1
    file = File.new("#{@directory}/x_#{count}.sql", 'w')
    file.write header
    file
  end

  def reset_file(file, count, header)
    file.close
    start_file(count, header)
  end
end

require 'strscan'

class HeaderParser
  # The headers for these databases are interesting
  # Structure:
  # type:length:content
  # (i.e. s:11:"hello world")
  #
  # There are three types I've identified so far:
  # a: array
  # s: string
  # i: integer
  #
  # The length is important because quotes are not escaped in strings
  # Basically scan, find the first character, if it'a an array it's
  # a:3:{, so tuck that away, and we know how many elements to look for
  # Then string is s:3:"sls" so we can start doing that as well, building
  # up the structure

  @@types = {'a': Array, 's': String, 'i': Integer}
  @@Element = Struct.new(:class, :parent, :length, :content, :raw)
  def initialize(header)
    @header = header

    # The structure is always (seemingly) an array at the top, so we
    # initialize it here
    @structure = []
  end

  def parse(timeout = 1)
    #StackProf.run(raw: true, mode: :cpu, out: 'tmp/stackprof-cpu-parser.dump') do
    @scanner = StringScanner.new(@header)
    # Let's start scanning!
    @current_element = nil
    @parent_array = nil

    timer = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    # Each pass should deal with a complete string or integer
    # If it's an array we set "parent array" and bail early
    until @scanner.eos? do
      # Get the next character
      @char = @scanner.getch

      # Intialize for this pass
      @current_element = @@Element.new(nil, nil, nil, nil, '')

      # If the parent_array and type_of_current_object this is the first pass through
      @current_element.class = check_type(@char) if @type_of_current_object.nil?

      # Get the length, except as an integer, because it's just a number
      next_char
      @current_element.length = parse_length unless @current_element.class.to_s == Integer.to_s

      # Here we still don't care what the type is yet!

      # We set the parent of the object to the current array (or nothing if it's nil)
      @current_element.parent = @parent_array
      # Check if the parent_array's content is nil, if it is, make it an array
      if @parent_array
        @parent_array.content = [] if @parent_array.content.nil?
        # Now we add the object to the content of the array
        @parent_array.content << @current_element
      end

      # Now we care if it's an array. If so, we need to set it to the current array
      if @current_element.class.to_s == Array.to_s
        @parent_array = @current_element
        # We then skip past the : and {
        next_char
        # We just want to start back up at the top now
        next
      end

      # We're sure it's an object here, probably string
      case @current_element.class.to_s
      when String.to_s
        @current_element.content = parse_content_string @current_element.length
        # if @current_element.content.start_with? "8bit"
        #   byebug
        # end

      when Integer.to_s
        @current_element.content = parse_content_integer
      else
        raise "Invalid type #{@current_element.class.to_s} found"
      end

      # For the moment, let's see what's going on
      # We seem to be on track!
      # Now we need to check for the ; that ends the element
      # There's a weird bug where the serialization may have screwed up.
      # If we have to go past more than say... 5 characters, let's instead go backwards
      # Let's try 10 characters instead.
      check_count = 0
      while @char != ';' && @char.nil? == false do
        next_char
        check_count += 1
      end

      # This check stuff works if the section is LOWER than the amount we found. But not greater.
      # So.... What can we do....
      #
      # Let's go backwards if we hit count
      if check_count > 10 && check_count < @current_element.length
        previous_char
        while @char != ';' && @char.nil? == false do
          previous_char
        end
      end

      # Now that we're at the end of the element, let's check if we should end this array
      if look_ahead == '}'
        # We need to close off the current array
        @parent_array = @parent_array.parent unless @parent_array.parent.nil?
        # Skip over the } that'll be there to close it
        while @char != '}' do
          next_char
        end
      end

      # raise(RuntimeError, "Timeout") if Process.clock_gettime(Process::CLOCK_MONOTONIC) - timer > timeout
    end
#    end
    # Clear this out
    return self.to_hash @parent_array unless @parent_array.nil?
    {}

  rescue TypeError => e
    # It seems as if there was a limit on the column when first stored, so sometimes the
    # string just cuts off and is not valid to be unserialized. This catches that and wraps
    # everything up nicely anyways.
    hash = self.to_hash @parent_array
    hash['error'] = e
    hash
  ensure
    @current_element = nil
    @parent_array = nil
    @scanner = nil
  end
#  rescue RuntimeError => e
#    hash = self.to_hash @parent_array
#    hash['error'] = e
#    hash
#  end

  def to_hash array
    # So the top element always seems to be an array, raise if it's not
    raise "Top element not an array" unless @parent_array.class.to_s == Array.to_s
    # However, we actually want it to be a hash
    hash = {}
    # Now we go through the content
    # arrays don't have keys, so we'll just make it "array0" "array1" etc.
    array_index = 0
    # the key we're looking at
    key = nil

    return hash if array.content.nil?
    array.content.each do |item|
      # If we don't have a key we want to make one
      # If it's an array just then we just create a filler
      if item.class.to_s == Array.to_s
        key = "array#{array_index}"
        hash[key] = self.to_hash item
        key = nil
        next
      elsif key.nil?
        # if the key is a string set the key, then move to the next element
        key = item.content
        next
      end

      # Otherwise just set the key to the content
      item_content = item.content
      hash[key] = item_content
      item_content = nil
      key = nil
    end

    hash
  end

  def parse_content
    # content depends on the type of the current element
    case @current_element.class.to_s
    when Integer.to_s
      parse_content_integer
    when String.to_s
      parse_content_string
    when Array.to_s
      parse_content_array
    end
  end

  def parse_content_integer
    # Since it's an integer we can just add to 0
    integer = 0
    # Move past the : character
    @char = next_char
    # Until we get to the end of this
    # We throw an error unless it's a number
    while @char != ';' do
      integer += Integer(@char)
      next_char
    end
    # All is good in the world, let's move on
    integer
  end

  def parse_content_string(length = @current_element.length)
    # Set the string we'll be appending to
    string = ""
    # Move past the " character
    next_char
    next_char if @char == '\\'
    # " aren't escaped or anything so we have to just loop through the length
    length.times do
      next_character = next_char
      break if next_character.nil?
      string << next_character
    end

    while look_ahead(2).include?('"') == false
      string << next_char
    end

    # However! There are issues with encoding, where it's possible that the subject is empty even
    # if the length is long.
    # In this case, we can check if the string starts with spaces followed by '\"'
    # byebug
    # puts "checking:"
    # puts string

    if string.match(/^[\s]+\S\"\;/) || string.match(/^\"\;[a-z]:[\d]+/)
      # Then we rewind `length` characters
      previous_char length
      # Go forward until we get to a "
      while look_ahead != '"'
        next_char
      end
      # byebug
      string = "ERROR PARSING"
    end
    # move past the " and ;
    next_char 2
    # All is good in the world let's move on
    string
  end

  def parse_content_array(length = @current_element.length)

    # Move past the : and {
    next_char 2
    # With another array, we want to dig down, so we set
    # the current element to this one, and set the parent
    # to the previous one.
    @current_element = @@Element.new Array, @current_element, length, nil, ""
  end

  def parse_length
    length = ''
     # Loop through until we get to the next color
     while true
       @char = next_char
       break if @char == ':'
       length << @char
     end
     # Change the length to an integer, save it
     # @current_element.length = length.to_i
     length.to_i
  end

  # Moves the pointer while also adding to the raw of the current element, mostly for debugging purposes
  def next_char(length = 1)
    length.times do
      @char = @scanner.getch
      if @char.nil?
        break
      else
        @current_element.raw << @char
      end
    end

    @char
  end

  # Moves the pointer backwards a certain amount
  def previous_char(length = 1)
    position = @scanner.pos
    position -= length + 1
    @scanner.pos = position
    @char = @scanner.getch
  end

  # Looks to the next character without moving the scanner pointer
  def look_ahead(length = 1)
    @scanner.peep length
  end

  # Checks if it's a string, array, or integer. Throws error if something else is in there somehow.
  def check_type(character)
    result = @@types[character.to_sym]

    # Because some things are not properly closed
    if result.nil? && character != "}"
      raise "Cannot find type #{character}"
    end
    result
  end
end

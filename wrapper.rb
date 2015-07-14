require 'ffi'

class PtrWrapper
  def initialize(ptr)
    @ptr = ptr
  end

  def get_pointer
    @ptr
  end
end

class DBWrapper < PtrWrapper
end

module TagDBWrapper


  extend FFI::Library
  ffi_lib 'libtag_db.so'

  attach_function :new_db_intern, :new_db, [], :pointer
  def self.new_db()
    DBWrapper.new(self.new_db_intern)
  end

  attach_function :delete_db_intern, :delete_db, [:pointer], :void
  def self.delete_db(ptr)
    raise unless ptr.is_a? DBWrapper
    delete_db_intern(ptr.get_pointer)
  end

  def self.range_to_min_max(range)
    raise unless range.min <= range.max
    raise unless range.min >= 0
    raise unless range.max < 2**64
    return range.min, range.max
  end

  attach_function :insert_db_intern, :insert_db, [:pointer, :string, :uint64, :uint64, :uint64, :pointer], :void
  def self.insert_db(db, name, range, data)
    raise unless db.is_a? DBWrapper
    raise unless
    memBuf = FFI::MemoryPointer.new(:char, data.size) # Create a memory pointer sized to the data
    memBuf.put_bytes(0, data)                         # Insert the actual data 
    min,max = self.range_to_min_max(range)
    insert_db_intern(db.get_pointer, name, min, max, data.size, memBuf)
  end

  attach_function :query_db_intern, :query_db, [:pointer, :string, :uint64, :uint64], :pointer
  def self.query_db(db, table, range)
    raise unless db.is_a? DBWrapper
    raise unless table.is_a? String
    raise unless range.is_a? Range
    min,max = self.range_to_min_max(range)
    QueryWrapper.new(db.get_pointer, self.query_db_intern(db.get_pointer, table, min, max))
  end

  attach_function :delete_query_intern, :delete_query, [:pointer, :pointer], :void
  def self.delete_query(query)
    raise unless query.is_a? QueryWrapper
    self.delete_query_intern(query.get_db, query.get_pointer)
  end

  attach_function :has_some_query_intern, :has_some_query, [:pointer], :uint8
  def self.has_some_query(query)
    raise unless query.is_a? QueryWrapper
    res=self.has_some_query_intern(query.get_pointer)
    return true if res == 1
    return false if res == 0
    raise "unexpected result #{res}"
  end

  #attach_function :get_key_iter_intern, :get_key_iter, [:pointer], :pointer
  #attach_function :get_val_iter_intern, :get_key_iter, [:pointer,:pointer], :pointer

  attach_function :get_data_query_intern, :get_data_query, [:pointer,:pointer], :pointer
  def self.get_data_query(query)
    raise unless query.is_a? QueryWrapper
    size_buffer = FFI::MemoryPointer.new(8)
    data_buffer = self.get_data_query_intern(query.get_pointer, size_buffer)
    size = size_buffer.read_bytes(8).unpack("Q").first
    size_buffer.free
    return data_buffer.read_bytes(size)
  end

  attach_function :get_key_query_intern, :get_key_query, [:pointer,:pointer], :void
  def self.get_key_query(query)
    raise unless query.is_a? QueryWrapper
    range_buffer = FFI::MemoryPointer.new(16)
    self.get_key_query_intern(query.get_pointer, range_buffer)
    from,to = *range_buffer.read_bytes(16).unpack("QQ")
    range_buffer.free
    return from..to
  end

  attach_function :next_item_query_intern, :next_item_query, [:pointer], :void
  def self.next_item_query(query)
    raise unless query.is_a? QueryWrapper
    self.next_item_query_intern(query.get_pointer)
  end
end

class QueryWrapper < PtrWrapper
    def initialize(db, ptr)
      super(ptr)
      @db = db
    end

    def get_db
      @db
    end

    def delete!
      TagDBWrapper.delete_query(self)
    end

    def each_pair
      return Enumerator.new do |y|
        while(TagDBWrapper.has_some_query(self))
          y << [TagDBWrapper.get_key_query(self), TagDBWrapper.get_data_query(self)]
          TagDBWrapper.next_item_query(self)
        end
      end
    end

    def each_key
      return Enumerator.new do |y|
        while(TagDBWrapper.has_some_query(self))
          y << TagDBWrapper.get_key_query(self)
          TagDBWrapper.next_item_query(self)
        end
      end
    end

    def each_value
      return Enumerator.new do |y|
        while(TagDBWrapper.has_some_query(self))
          y << TagDBWrapper.get_data_query(self)
          TagDBWrapper.next_item_query(self)
        end
      end
    end

  end

class TagDB
  def initialize
    @wrapped = TagDBWrapper.new_db
  end

  def delete!
    TagDBWrapper.delete_db(@wrapped)
  end

  def insert(name,range,val)
    TagDBWrapper.insert_db(@wrapped, name, range, val)
  end

  def query(name,range)
    begin
      query = TagDBWrapper.query_db(@wrapped,name,range)
      return yield(query)
    ensure
      query.delete!
    end
    return nil
  end
end

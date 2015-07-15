require_relative './wrapper.rb'

db = TagDBWrapper.new_db()

iter = TagDBWrapper.query_db(db, "fnord",0..1000)
raise if TagDBWrapper.has_some_query(iter)
TagDBWrapper.delete_query(iter)

TagDBWrapper.insert_db(db, "fnord", 4..8, "hallo\0welt" )

iter = TagDBWrapper.query_db(db, "fnord",0..1000)
raise unless TagDBWrapper.has_some_query(iter)
raise unless TagDBWrapper.get_data_query(iter) == "hallo\0welt"
raise unless TagDBWrapper.get_key_query(iter) == (4..8)
TagDBWrapper.next_item_query(iter)
raise if TagDBWrapper.has_some_query(iter)

TagDBWrapper.delete_query(iter)

TagDBWrapper.delete_db(db)


db = TagDB.new

100_000_0.times do 
  min =rand(100_000)
  max = min + rand(100_000)
  db.insert("tbl",min..max,"test4..8")
end
db.delete!

db = TagDB.new
db.insert("test",10..15,"fnord1")
db.insert("test",11..15,"fnord2")
db.insert("test",12..15,"fnord3")
query_res = db.query("test",0..100) { |f| f.each_pair.to_a }
raise unless query_res.length == 3
db.save_to_file("/tmp/test_dump.mp")
db2 = TagDB.load_from_file("/tmp/test_dump.mp")
query2_res = db2.query("test",0..100) { |f| f.each_pair.to_a }

raise unless query_res == query2_res

#puts db.query("tbl",0..100) { |f| f.each_pair.to_a }.inspect
#puts db.query("tbl",0..100) { |f| f.each_key.to_a }.inspect
#puts db.query("tbl",0..100) { |f| f.each_value.to_a }.inspect

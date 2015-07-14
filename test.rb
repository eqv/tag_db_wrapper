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
#db.insert("tbl",5..7,"test5..7")
#db.insert("tbl",6..6,"test6..6")
#
#puts db.query("tbl",0..100) { |f| f.each_pair.to_a }.inspect
#puts db.query("tbl",0..100) { |f| f.each_key.to_a }.inspect
#puts db.query("tbl",0..100) { |f| f.each_value.to_a }.inspect

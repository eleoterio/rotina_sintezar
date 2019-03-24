require "json"
require "mongo"

client = Mongo::Client.new([ 'localhost:27017' ], :database => 'linx')

collection = client['produto']

a = File.readlines("input-dump")
a.each do |linha|
    filter = JSON.parse(linha)
    image = filter["image"]
    name_image =  image.split("/")[4].split(".")[0]
    if name_image.to_i % 5 != 0
        collection.insert_one(filter)
    end 
end
aggregation = collection.aggregate([
    { '$group' => { '_id' => '$productId', 'products' => { '$push' => '$image' } } }
])
parse = []
aggregation.each do |doc|
    array = [
        'productId' => doc['_id'],
        'image' => doc['products'].first(3)
    ]
    parse.push(array.to_json)
end
puts parse
new = File.new("output-dump", "w")
new.puts(parse)
new.close

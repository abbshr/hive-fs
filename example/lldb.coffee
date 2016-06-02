level = require 'level'
db = level '/tmp/lldb.db', valueEncoding: 'json'

data =
  ts: Date.now()
  name: 'Ran Aizen'
  id: 1237104
  activeDay: [1,3,5,7,9]
  following: ['@_the_flash', '@_batman', '@_the_arrow', '@_superman']
  
# d = (k) ->
#   # db.put "github:#{k}", data, ->
#   #   console.log "github:#{k} write finished"
#   db.get "github:#{k}", (_, d) ->
#     console.log d.following
#     #   db.del "github:#{k}", ->
#     #     console.log "deleted"

# console.time 0
# d i for i in [1..10000]
# console.timeEnd 0
# process.on 'SIGINT', -> 
#   db.close ->
#     console.log 'closed'
#     process.exit()
console.time 0
db.createReadStream()
  .on 'data', ({key, value}) -> console.log key
  .on 'end', -> 
    console.log 'end'
    console.timeEnd 0
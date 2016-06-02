{Hive} = require '..'

hive = new Hive dirname: '/tmp', basename: 'hive-fs'

data =
  ts: Date.now()
  name: 'Ran Aizen'
  id: 1237104
  activeDay: [1,3,5,7,9]
  following: ['@_the_flash', '@_batman', '@_the_arrow', '@_superman']
  
exports.data = data
exports.hive = hive

# d = (k)->
#   # hive.write "github:#{k}", data, (err) ->
#   #   console.info "github:#{k} write finished"
#   hive.seek "github:#{k}", (err, value) ->
#     console.log value.following
#     # hive.free "github:#{k}", (err) ->
#     #   console.log "deleted github:#{k}"

console.time 0
# d i for i in [1..10000]
# console.timeEnd 0
# process.on 'SIGINT', ->
#   hive.close -> 
#     console.log 'closed'
#     process.exit()
hive.match()
  .on 'data', (idx, d) -> console.log idx
  .on 'end', -> 
    console.log 'end'
    console.timeEnd 0
    hive.close -> 
      console.log 'closed'
      process.exit()
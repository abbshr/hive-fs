{openSync, readSync, write, read, fstatSync, close, writeFile} = require 'fs'
{TUPLESTATE_SIZE, SEEK_SIZE, FRAGMENTLEN_SIZE, TIMESTAMP_SIZE, IDX_SIZE, NULL, FREE, USED} = require './constants'

TUPLE_SIZE = 1 + SEEK_SIZE + FRAGMENTLEN_SIZE + IDX_SIZE

class Meta

  constructor: (args) ->
    {file: @path}=  args
    @_seekFile = openSync @path, "a+"
    @_size = @updateSize()
    @_tupleCount = @_size // TUPLE_SIZE
    @_cache = {}
    @_freelst = []
    @_init() if @_size > 0

  _init: ->
    buffer = Buffer @_size
    readSync @_seekFile, buffer, 0, @_size, 0
    [@_cache, @_freelst] = @_unpack buffer

  updateSize: ->
    {size} = fstatSync @_seekFile
    size

  _getAvailiable: ->
    if @_freelst.length > 0
      pos = @_freelst.pop()
      [pos * TUPLE_SIZE, pos]
    else
      [@_size, @_tupleCount++]

  ensure: (idx, seek, len, callback) ->
    if idx of @_cache
      @_updateMeta idx, seek, len, callback
    else
      tuple = Buffer TUPLE_SIZE
      idxData = @_pack tuple, idx, seek, len, USED
      [location, pos] = @_getAvailiable()
      @_cache[idx] = {seek, len, pos}
      @_size += idxData.length if location is @_size
      write @_seekFile, idxData, 0, idxData.length, location, (err) ->
        callback err

  _updateMeta: (idx, seek, len, callback) ->
    meta = Buffer SEEK_SIZE + FRAGMENTLEN_SIZE
    meta.writeUInt32BE seek
    meta[SEEK_SIZE] = len
    # meta.writeDoubleBE timestamp, SEEK_SIZE + FRAGMENTLEN_SIZE

    location = @_cache[idx].pos * TUPLE_SIZE + 1
    Object.assign @_cache[idx], {seek, len}
    
    write @_seekFile, meta, 0, meta.length, location, (err) ->
      callback err

  seekfor: (idx, callback) ->
    # {seek} = @_cache[idx]
    callback null, @_cache[idx]
    # if seek?
    #   callback null, @_cache[idx]
    # else
    #   oldsize = @_size
    #   @_size = @_getSize()
    #   diffSize = @_size - oldsize
    #   return callback null, null if diffSize is 0
    #   blk = Buffer diffSize
    #   read @_seekFile, blk, 0, diffSize, oldsize, (err, byte, buffer) =>
    #     if err?
    #       callback err, null
    #     else
    #       [tmpcache, tmpfreelst] = @_unpack buffer
    #       tuple.pos += @_tupleCount for _, tuple of tmpcache
    #       Object.assign @_cache, tmpcache
    #       @_freelst.push (@_tupleCount + i for i in tmpfreelst)...
    #       @_tupleCount = @_size // TUPLE_SIZE
    #       callback null, @_cache[idx]

  drop: (idx, callback) ->
    if idx of @_cache
      freepos = @_cache[idx].pos
      @_freelst.push freepos
      location = freepos * TUPLE_SIZE
      delete @_cache[idx]
      state = Buffer [FREE]
      write @_seekFile, state, 0, 1, location, (err) ->
        callback err
    else
      callback null

  _pack: (tuple, idx, seek, len, state) ->
    tuple[0] = state
    tuple.writeUInt32BE seek, 1
    tuple[5] = len
    # tuple.writeDoubleBE timestamp, 6
    offset = 6 + tuple.utf8Write idx, 6
    tuple.fill NULL, offset if offset < tuple.length

  _unpack: (buffer) ->
    [idxlst, freelst] = [{}, []]

    for _, i in buffer by TUPLE_SIZE
      state = buffer[i]
      pos = i // TUPLE_SIZE
      if state is FREE
        freelst.push pos
      else
        seek = buffer.readUInt32BE i + 1
        len = buffer[i + 5]
        # timestamp = buffer.readDoubleBE i + 6
        nullbyte = buffer.indexOf NULL, i + 6
        idx = buffer.toString 'utf-8', i + 6, nullbyte if !!~nullbyte
        idxlst[idx] = {seek, len, pos}

    [idxlst, freelst]

  close: (callback = ->) -> 
    unless @_closed
      @_closed = yes
      @_arrange => close @_seekFile, (err) =>
        callback err
        
  _arrange: (callback = ->) ->
    @_tupleCount = @_tupleCount - @_freelst.length
    @_freelst = []
    
    @_size = @_tupleCount * TUPLE_SIZE
    dirty = Buffer @_size
    i = 0
    for idx, info of @_cache
      {seek, len} = info
      info.pos = i // TUPLE_SIZE
      tuple = dirty[i ... i = i + TUPLE_SIZE]
      @_pack tuple, idx, seek, len, USED

    writeFile @path, dirty, callback

module.exports = Meta

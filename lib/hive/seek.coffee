{openSync, readSync, write, read, fstatSync, close} = require 'fs'
{TUPLESIZE, NULL, FREE, USED} = require '../constants'

class Index

  # TUPLESIZE: 270
  # NULL: 0x00
  # FREE: 0x00
  # USED: 0x01

  constructor: (args) ->
    flag = if args.producer then 'a+' else 'r'
    @_seekFile = openSync args.file, flag
    @_size = @updateSize()
    @_tupleCount = @_size // TUPLESIZE
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
      [pos * TUPLESIZE, pos]
    else
      [@_size, @_tupleCount++]

  ensure: (idx, seek, len, timestamp, callback) ->
    if @_flag is 'r'
      return callback new Error "Can not ensure Index in Consumer mode"

    if idx of @_cache
      @_updateMeta idx, seek, len, timestamp, callback
    else
      tuple = Buffer TUPLESIZE
      idxData = @_pack tuple, idx, seek, len, timestamp, USED
      [location, pos] = @_getAvailiable()
      @_cache[idx] = {seek, len, timestamp, pos}

      write @_seekFile, idxData, 0, idxData.length, location, (err, byte) =>
        unless err?
          @_size += byte if location is @_size
        callback err

  _updateMeta: (idx, seek, len, timestamp, callback) ->
    meta = Buffer 9
    meta.writeUInt32BE seek
    meta[4] = len
    meta.writeDoubleBE timestamp, 5

    location = @_cache[idx].pos * TUPLESIZE + 1
    Object.assign @_cache[idx], {seek, len, timestamp}

    write @_seekFile, meta, 0, meta.length, location, (err, byte) =>
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
    #       @_tupleCount = @_size // TUPLESIZE
    #       callback null, @_cache[idx]

  drop: (idx, callback) ->
    if idx of @_cache
      freepos = @_cache[idx].pos
      @_freelst.push freepos
      location = freepos * TUPLESIZE
      delete @_cache[idx]
      state = Buffer [FREE]
      write @_seekFile, state, 0, 1, location, (err, byte) =>
        callback err
    else
      callback null

  _pack: (tuple, idx, seek, len, timestamp, state) ->
    tuple[0] = state
    tuple.writeUInt32BE seek, 1
    tuple[5] = len
    tuple.writeDoubleBE timestamp, 6
    offset = 14 + tuple.utf8Write idx, 14
    tuple.fill NULL, offset if offset < tuple.length

  _unpack: (buffer) ->
    [idxlst, freelst] = [{}, []]

    for _, i in buffer by TUPLESIZE
      state = buffer[i]
      pos = i // TUPLESIZE
      if state is FREE
        freelst.push pos
      else
        seek = buffer.readUInt32BE i + 1
        len = buffer[i + 5]
        timestamp = buffer.readDoubleBE i + 6
        nullbyte = buffer.indexOf NULL, i + 14
        idx = buffer.toString 'utf-8', i + 14, nullbyte if !!~nullbyte
        idxlst[idx] = {seek, len, timestamp, pos}

    [idxlst, freelst]

  close: (callback = ->) -> close @_seekFile, (err) => @_arrange callback

  _arrange: (callback = ->) ->
    @_tupleCount = count = @_tupleCount - @_freelst.length
    @_size = @_tupleCount * TUPLESIZE
    return callback() if count is 0

    dirty = Buffer count * TUPLESIZE
    i = 0
    for idx, info of @_cache
      {seek, len, timestamp} = info
      info.pos = i // TUPLESIZE
      tuple = dirty[i ... i = i + TUPLESIZE]
      @_pack tuple, idx, seek, len, timestamp, USED

    fs.writeFile @_file, dirty, (err) -> callback err

module.exports = Index

{openSync, write, read, fstatSync, close} = require 'fs'

class Index

  TUPLESIZE: 270
  NULL: 0x00
  FREE: 0x00
  USED: 0x01

  constructor: (args) ->
    @_cache = {}
    @_freelst = []
    @_flag = if args.producer then 'a+' else 'r'
    @_file = args.file

  init: (callback) ->
    @_seekFile = openSync @_file, @_flag
    @_size = @_getSize()
    @_tupleCount = @_size // Index::TUPLESIZE
    if @_size is 0
      @_cache = {}
      @_freelst = []
      callback()
    else
      @_preRead (..., buffer) =>
        [@_cache, @_freelst] = @_unpack buffer
        callback()

  _getSize: ->
    {size} = fstatSync @_seekFile
    size

  _preRead: (callback) ->
    read @_seekFile, Buffer(@_size), 0, @_size, 0, callback

  _getAvailiable: ->
    if @_freelst.length > 0
      pos = @_freelst.pop()
      [pos * Index::TUPLESIZE, pos]
    else
      [@_size, @_tupleCount++]

  ensure: (idx, seek, fragment, timestamp, callback) ->
    if @_flag is 'r'
      return callback new Error "Can not ensure Index in Consumer mode"

    if idx of @_cache
      @_updateMeta idx, seek, fragment, timestamp, callback
    else
      tuple = Buffer Index::TUPLESIZE
      idxData = @_pack tuple, idx, seek, fragment, timestamp, Index::USED
      [location, pos] = @_getAvailiable()
      @_cache[idx] = {seek, fragment, timestamp, pos}

      write @_seekFile, idxData, 0, idxData.length, location, (err, byte) =>
        unless err?
          @_size += byte if location is @_size
        callback err

  _updateMeta: (idx, seek, fragment, timestamp, callback) ->
    meta = Buffer 9
    meta[0..3].writeUInt32BE seek
    meta[4] = fragment
    meta[5..12].writeDoubleBE timestamp

    location = @_cache[idx].pos * Index::TUPLESIZE + 1
    Object.assign @_cache[idx], {seek, fragment, timestamp}

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
    #       @_tupleCount = @_size // Index::TUPLESIZE
    #       callback null, @_cache[idx]

  drop: (idx, callback) ->
    if idx of @_cache
      freepos = @_cache[idx].pos
      @_freelst.push freepos
      location = freepos * Index::TUPLESIZE
      delete @_cache[idx]
      state = Buffer [Index::FREE]
      write @_seekFile, state, 0, 1, location, (err, byte) =>
        callback err
    else
      callback null

  _pack: (tuple, idx, seek, fragment, timestamp, state) ->
    tuple[0] = state
    tuple[1..4].writeUInt32BE seek
    tuple[5] = fragment
    tuple[6..13].writeDoubleBE timestamp
    tuple[14..].fill(Index::NULL).utf8Write idx
    tuple

  _unpack: (buffer) ->
    [idxlst, freelst] = [{}, []]

    for _, i in buffer by Index::TUPLESIZE
      tuple = buffer[i ... i + Index::TUPLESIZE]
      state = tuple[0]
      pos = i // Index::TUPLESIZE
      if state is Index::FREE
        freelst.push pos
      else
        seek = tuple[1..4].readUInt32BE 0
        fragment = tuple[5]
        timestamp = tuple[6..13].readDoubleBE 0
        idx = tuple[14..]
        nullbyte = idx.indexOf Index::NULL
        idx = idx[0...nullbyte] if !!~nullbyte
        idx = idx.toString 'utf-8'
        idxlst[idx] = {seek, fragment, timestamp, pos}

    [idxlst, freelst]

  close: (callback = ->) -> close @_seekFile, (err) => @_arrange callback

  _arrange: (callback = ->) ->
    @_tupleCount = count = @_tupleCount - @_freelst.length
    @_size = @_tupleCount * Index::TUPLESIZE
    return callback() if count is 0

    dirty = Buffer count * Index::TUPLESIZE
    i = 0
    for idx, info = {seek, fragment, timestamp} of @_cache
      info.pos = i // Index::TUPLESIZE
      tuple = dirty[i ... i = i + Index::TUPLESIZE]
      @_pack tuple, idx, seek, fragment, timestamp, Index::USED

    fs.writeFile @_file, dirty, (err) -> callback err

module.exports = Index

{openSync, write, read, fstatSync, close} = require 'fs'

class Index

  TUPLESIZE: 262
  NULL: 0

  constructor: (args) ->
    @_cache = {}
    @_freelst = []
    @_flag = if args.producer then 'a+' else 'r'
    @_file = args.file

  init: (callback) ->
    @_seekFile = openSync @_file, @_flag
    @_size = @_getSize()
    @_tupleCount = @_size // Index::TUPLESIZE
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
      num = @_freelst.pop()
      [num * Index::TUPLESIZE, num]
    else
      [@_size, ++@_tupleCount]

  ensure: (idx, seek, fragment, callback) ->
    if @_flag is 'r'
      callback new Error "Can not ensure Index in Consumer mode"
    else
      idxData = @_pack idx, seek, fragment
      [location, num] = @_getAvailiable()
      write @_seekFile, idxData, 0, idxData.length, location, (err, byte) =>
        unless err?
          @_cache[idx] = {seek, fragment, num}
          @_size += byte if location is @_size
        callback err

  update: (idx, seek, fragment, callback) ->
    if @_flag is 'r'
      callback new Error "Can not ensure Index in Consumer mode"
    else
      seekfor idx, (err, info) =>
        {seek} = info
        if seek?
          idxData = @_pack idx, seek, fragment
          location = @_cache[idx].num * Index::TUPLESIZE
          write @_seekFile, idxData, 1, 5, location, (err, byte) =>
            callback err
        else
          callback null

  seekfor: (idx, callback) ->
    {seek, fragment} = @_cache[idx]
    if seek?
      callback null, @_cache[idx]
    else
      oldsize = @_size
      @_size = @_getSize()
      diffSize = @_size - oldsize
      return callback null, null if diffSize is 0
      blk = Buffer diffSize
      read @_seekFile, blk, 0, diffSize, oldsize, (err, byte, buffer) =>
        if err?
          callback err, null
        else
          [tmpcache, tmpfreelst] = @_unpack buffer
          tuple.num += @_tupleCount for _, tuple of tmpcache
          Object.assign @_cache, tmpcache
          @_freelst.push (@_tupleCount + i for i in tmpfreelst)...
          @_tupleCount = @_size // Index::TUPLESIZE
          callback null, @_cache[idx]

  drop: (idx, callback) ->
    seekfor idx, (err, seek) =>
      if seek?
        @_freelst.push @_cache[idx].num
        delete @_cache[idx]
        freeflag = Buffer [0x01]
        write @_seekFile, freeflag, 0, 1, seek, (err, byte) =>
          callback err

  _pack: (idx, seek, fragment = 1, free = 0) ->
    tuple = Buffer Index::TUPLESIZE
    tuple.fill Index::NULL
    tuple[0].writeUInt8 free
    tuple[1..4].writeUInt32BE seek
    tuple[5].writeUInt8 fragment
    tuple[6..].utf8Write idx
    tuple

  _unpack: (buffer) ->
    [idxlst, freelst] = [{}, []]

    for tuple, i in buffer by Index::TUPLESIZE
      free = tuple[0]
      if free
        freelst.push i // Index::TUPLESIZE
      else
        seek = tuple[1..4]
        fragment = tuple[5]
        idx = tuple[6..].toString 'utf-8'
        idxlst[idx] = {seek, fragment, num: i}

    [idxlst, freelst]

  close: (callback = ->) ->
    close @_seekFile, callback

module.exports = Index

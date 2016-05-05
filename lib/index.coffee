{openSync, readSync, write, read, fstatSync, closeSync, writeFile} = require 'fs'
{EventEmitter} = require 'events'
path = require 'path'
Meta = require './meta'
Slot = require './slot'
{unorderList} = require 'archangel-util'
{UNIT_SIZE, BLK_SIZE} = require './constants'

class Hive extends EventEmitter
  # parameters
  # @dirname: '/tmp'
  # @basename: 'hive-fs'
  constructor: (args = {}) ->
    super()
    dirname = args.dirname ? '/tmp'
    basename = args.basename ? 'hive-fs'
    baseFile = path.join dirname, basename
    @path = recordFile = "#{baseFile}.rcd"
    seekFile = "#{baseFile}.idx"
    slotFile = "#{baseFile}.slot"

    @index = new Meta file: seekFile
    @slot = new Slot file: slotFile
    # @slot.size = 
    @_recordFile = openSync recordFile, 'a+'
    @_size = @_updateSize()
    @_freeSlotLst = []
    @_blklenMap = []
    @_init() if @_size > 0
    @_intervalWriteback()

  _init: ->
    buffer = Buffer @_size
    readSync @_recordFile, buffer, 0, @_size, 0
    closeSync @_recordFile
    @_freeSlotLst = @_unpack buffer
    @_blklenMap = @_yieldFreemap @_freeSlotLst

  _updateSize: ->
    {size} = fstatSync @_recordFile
    size

  _levelLstAppend: (map, level, elem) ->
    map[level] ?= []
    map[level].push elem

  _cellBlklen: (size) ->
    Math.ceil size / BLK_SIZE

  _yieldFreemap: (freeSlotLst) ->
    freemap = []
    for item, curr in freeSlotLst when item?
      @_levelLstAppend freemap, item.len, curr
    freemap

  _unpack: (buffer) ->
    freelst = []
    curr = prev = next = null

    if buffer.length is UNIT_SIZE
      curr = buffer.readUInt32BE 0
      len = buffer.readUInt32BE 4
      freelst[curr] = {prev, next, len}
      return freelst

    for _, i in buffer[...-UNIT_SIZE] by UNIT_SIZE
      next = buffer.readUInt32BE i + UNIT_SIZE
      curr = buffer.readUInt32BE i
      len = buffer.readUInt32BE i + 4
      freelst[curr] = {prev, next, len}
      prev = curr

    curr = buffer.readUInt32BE i
    len = buffer.readUInt32BE i + 4
    next = null
    freelst[curr] = {prev, next, len}
    freelst

  _pack: (buffer, i, curr, len) ->
    buffer.writeUInt32BE curr, i
    buffer.writeUInt32BE len, i + 4

  _mergeFreeblk: (seek, len) ->
    # slot offset
    curr = seek // BLK_SIZE
    next = pnext = null
    prev = nprev = null
    nnext = null
    pprev = null

    # fragment length
    nlen = NaN
    plen = NaN

    # append first free slot
    if @_freeSlotLst.length is 0
      @_freeSlotLst[curr] = {prev, next, len}
      @_levelLstAppend @_blklenMap, len, curr
      return

    # calculate the prev & next free continous-slots offset
    sublst = for item, i in @_freeSlotLst[curr..] when item?
      {prev: nprev, next: nnext, len: nlen} = item
      prev = nprev
      next = i + curr
      if prevItem = @_freeSlotLst[prev]
        {prev: pprev, len: plen} = prevItem
      break
    
    unless sublst.length > 0
      for i in [curr..0] when @_freeSlotLst[i]?
        {prev: pprev, len: plen} = @_freeSlotLst[i]
        prev = i
        break

    if next is prev is nnext is pprev is null
      @_freeSlotLst[curr] = {prev, next, len}
      @_levelLstAppend @_blklenMap, len, curr
      return
    
    if prev + plen is curr
      # merge with prev
      item = @_freeSlotLst[prev]
      item.len += len
      unorderList.rm @_blklenMap[plen], prev
      if curr + len is next
        # merge with prev & next
        item.next = nnext
        @_freeSlotLst[nnext]?.prev = prev
        item.len += nlen
        delete @_freeSlotLst[next]
        # @_freeSlotLst.splice next, 1
        unorderList.rm @_blklenMap[nlen], next

      @_levelLstAppend @_blklenMap, item.len, prev
    else
      item = @_freeSlotLst[curr] = {prev, next, len}
      @_freeSlotLst[prev]?.next = curr
      @_freeSlotLst[next]?.prev = curr
      if curr + len is next
        # merge with next
        item.next = nnext
        item.len += nlen
        @_freeSlotLst[nnext]?.prev = curr
        delete @_freeSlotLst[next]
        # @_freeSlotLst.splice next, 1
        unorderList.rm @_blklenMap[nlen], next

      @_levelLstAppend @_blklenMap, item.len, curr

  _getAvailiableSeek: (slotlen) ->
    for levellst in @_blklenMap[slotlen..] when levellst?.length
      curr = levellst.pop()
      {prev, next, len} = @_freeSlotLst[curr]
      delete @_freeSlotLst[curr]
      # @_freeSlotLst.splice curr, 1
      diff = len - slotlen
      if diff > 0
        newcurr = curr + slotlen
        @_freeSlotLst[newcurr] = {prev, next, len: diff}
        @_freeSlotLst[prev]?.next = @_freeSlotLst[next]?.prev = newcurr
        @_levelLstAppend @_blklenMap, diff, newcurr
      else
        @_freeSlotLst[prev]?.next = next
        @_freeSlotLst[next]?.prev = prev
      return curr * BLK_SIZE

    @slot.size

  _alloc: (idx, seek, blklen, chunk, callback) ->
    # console.log seek, blklen, chunk
    @index.ensure idx, seek, blklen, =>
      @slot.push seek, chunk, -> callback()

  write: (idx, value, callback) ->    
    chunk = @slot.alloc value
    {blklen, size} = chunk

    @index.seekfor idx, (err, info) =>
      if info?
        {seek, len} = info
        # return callback null if timestamp > ts
        diff = blklen - len
        if diff < 0
          freeSeek = seek + blklen * BLK_SIZE
          @_mergeFreeblk freeSeek, -diff
        else if diff > 0
          @_mergeFreeblk seek, len
          seek = @_getAvailiableSeek blklen
      else
        seek = @_getAvailiableSeek blklen

      @slot.incSize seek, size
      @_alloc idx, seek, blklen, chunk, callback

  seek: (idx, callback) ->
    @index.seekfor idx, (err, info) =>
      if info?
        {seek, len} = info
        # console.log seek, len
        @slot.skipto seek, len, callback
      else
        callback null, null

  free: (idx, callback) ->
    @index.seekfor idx, (err, info) =>
      if info?
        {seek, len} = info
        @_mergeFreeblk seek, len
        @index.drop idx, callback
      else
        callback null

  match: (wildcard) ->
    if wildcard? and wildcard isnt '*'
      regstr = wildcard.replace /[\+\-\^\$\?\.\{\}\[\]\|\,\(\)]/g, (o) -> "\\#{o}"
              .replace /\*/g, ".*"
      pattern = new RegExp "^#{regstr}$"
    
    for idx, {seek, len} of @index._cache when pattern?.test(idx) ? on
      @slot.skipto seek, len, do (idx) => (err, data) => @emit "data", idx, data

  close: (callback) ->
    return if @_closed
    @_closed = yes
    @index.close =>
      clearTimeout @_timer
      @_writeback => @slot.close callback

  _intervalWriteback: =>
    return if @_closed
    @_timer = setTimeout =>
      @_writeback @_intervalWriteback
    , 20 * 1000

  _writeback: (done) ->
    stash = ([num, item.len] for item, num in @_freeSlotLst when item?)
    return done() unless stash.length
    buffer = Buffer UNIT_SIZE * stash.length
    @_pack buffer, i * UNIT_SIZE, curr, len for [curr, len], i in stash
    writeFile @path, buffer, done

module.exports = Hive

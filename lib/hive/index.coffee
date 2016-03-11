{openSync, readSync, write, read, fstatSync, close} = require 'fs'
path = require 'path'
Index = require './seek'
Slot = require './slot'
{rm: unorderlstRm} = require '../util/unorderlst'
{UNITSIZE, BLKSIZE} = require '../constants'

class Hive
  # parameters
  # @dirname: '/tmp'
  # @basename: 'hive-fs'
  # @rw: true
  constructor: (args = {}) ->
    args.rw ?= yes
    dirname = args.dirname ? '/tmp'
    basename = args.basename ? 'hive-fs'
    baseFile = path.join dirname, basename
    recordFile = "#{baseFile}.rcd"
    seekFile = "#{baseFile}.idx"
    slotFile = "#{baseFile}.slot"

    @index = new Index producer: args.rw, file: seekFile
    @slot = new Slot producer: args.rw, file: slotFile

    @_recordFile = openSync recordFile, 'a+'
    @_size = @_updateSize()
    @_freeSlotLst = []
    @_blklenMap = []
    @_init() if @_size > 0

  _init: ->
    buffer = Buffer @_size
    readSync @_recordFile, buffer, 0, @_size, 0
    @_freeSlotLst = @_unpack buffer
    @_blklenMap = @_yieldFreemap @_freeSlotLst

  # _writeback: ->
  #   return if @_closed
  #   @_timer = setTimeout =>
  #     stash = ([num, item.len] for item, num in @_freeSlotLst when item?)
  #     buffer = Buffer UNITSIZE * stash.length
  #     @_pack unit, stash[i]... for unit, i in buffer by UNITSIZE
  #
  #     @_writeback()
  #   , 30 * 1000

  _updateSize: ->
    {size} = fstatSync @_recordFile
    size

  _levelLstAppend: (map, level, elem) ->
    map[level] ?= []
    map[level].push elem

  _cellBlklen: (size) ->
    Math.ceil size / BLKSIZE

  _yieldFreemap: (freeslotLst) ->
    freemap = []
    for item, curr in freeSlotLst when item?
      @_levelLstAppend freemap, item.len, curr
    freemap

  _unpack: (buffer) ->
    freelst = []
    curr = prev = next = null

    if buffer.length is UNITSIZE
      curr = buffer.readUInt32BE 0
      len = buffer[4]
      freelst[curr] = {prev, next, len}
      return freelst

    for _, i in buffer[...-UNITSIZE] by UNITSIZE
      next = buffer.readUInt32BE i + UNITSIZE
      curr = buffer.readUInt32BE i
      len = buffer[i + 4]
      freelst[curr] = {prev, next, len}
      prev = curr

    curr = buffer.readUInt32BE i
    len = buffer[i + 4]
    next = null
    freelst[curr] = {prev, next, len}
    freelst

  _pack: (buffer, curr, len) ->
    buffer.writeUInt32BE curr
    buffer[UNITSIZE - 1] = len

  _mergeFreeblk: (seek, len) ->
    # slot offset
    curr = seek // BLKSIZE
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
    sublst = @_freeSlotLst[curr..]
    if sublst.length > 0
      for item, i in sublst when item?
        {prev: nprev, next: nnext, len: nlen} = item
        prev = nprev
        next = i + curr
        if prevItem = @_freeSlotLst[prev]
          {prev: pprev, len: plen} = prevItem
        break
    else
      for i in [curr..0] when @_freeSlotLst[i]?
        {prev: pprev, len: plen} = @_freeSlotLst[i]
        prev = i
        break

    if prev + plen is curr
      # merge with prev
      item = @_freeSlotLst[prev]
      item.len += len
      unorderlstRm @_blklenMap[plen], prev
      if curr + len is next
        # merge with prev & next
        item.next = nnext
        item.len += nlen
        delete @_freeSlotLst[next]
        unorderlstRm @_blklenMap[nlen], next

      @_levelLstAppend @_blklenMap, item.len, prev
    else
      item = @_freeSlotLst[curr] = {prev, next, len}
      @_freeSlotLst[prev]?.next = curr
      @_freeSlotLst[next]?.prev = curr
      if curr + len is next
        # merge with next
        item.next = nnext
        item.len += nlen
        @_levelLstAppend @_blklenMap, item.len, curr
        delete @_freeSlotLst[next]
        unorderlstRm @_blklenMap[nlen], next

  _getAvailiableSeek: (slotlen) ->
    for levellst in @_blklenMap[slotlen..] when levellst?.length
      curr = levellst.pop()
      {prev, next, len} = @_freeSlotLst[curr]
      delete @_freeSlotLst[curr]
      diff = len - slotlen
      if diff > 0
        newcurr = curr + slotlen
        @_freeSlotLst[newcurr] = {prev, next, len: diff}
        @_freeSlotLst[prev]?.next = @_freeSlotLst[next]?.prev = newcurr
        @_levelLstAppend @_blklenMap, diff, newcurr
      else
        @_freeSlotLst[prev]?.next = next
        @_freeSlotLst[next]?.prev = prev
      return curr * BLKSIZE

    @slot.size

  _close: (callback) -> close @_recordFile, callback

  _alloc: (idx, seek, blklen, ts, cell, callback) ->
    @index.ensure idx, seek, blklen, ts, (err) =>
      # console.log 'ensure'
      @slot.push seek, cell, callback

  write: (data, callback) ->
    {idx, ts} = data
    unless idx? and ts?
      err = new Error "Index and TimeStamp not found"
      return callback err

    cell = @slot.alloc data
    {blklen} = cell

    @index.seekfor idx, (err, info) =>
      if info?
        {seek, len, timestamp} = info
        return callback null if timestamp > ts
        diff = blklen - len
        if diff < 0
          freeSeek = seek + blklen * BLKSIZE
          @_mergeFreeblk freeSeek, -diff
        else if diff > 0
          @_mergeFreeblk seek, len
          seek = @_getAvailiableSeek blklen
      else
        seek = @_getAvailiableSeek blklen
        # console.log seek, 'seek'

      @_alloc idx, seek, blklen, ts, cell, callback

  seek: (idx, callback) ->
    @index.seekfor idx, (err, info) =>
      if info?
        {seek, len} = info
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

  close: (callback) ->
    @index.close => @_close => @slot.close =>
      # @_closed = yes
      @_writeback -> callback()

module.exports = Hive

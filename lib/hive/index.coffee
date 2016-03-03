{openSync, write, read, fstatSync, close} = require 'fs'
Index = require './seek'
Slot = require './slot'
{rm: unorderlstRm} = require '../util/unorderlst'

class Hive

  UNITSIZE: 5

  constructor: (args) ->
    @_recordFile = openSync args.file, 'a+'
    @index = new Index args.indexcfg
    @slot = new Slot args.slotcfg
    @_timer = null

  init: (callback) ->
    @index.init => @slot.init => @_initFreeblk =>
      # write back to recordFile by a interval
      @_writeback()
      callback this

  _writeback: ->
    return if @_closed
    @_timer = setTimeout =>
      stash = ([num, item.len] for item, num in @_freeslotLst when item?)
      buffer = Buffer Hive::UNITSIZE * stash.length
      @_pack unit, stash[i]... for unit, i in buffer by Hive::UNITSIZE
      @_writeback()
    , 30 * 1000

  _getSize: ->
    {size} = fstatSync @_recordFile
    size

  _initFreeblk: (callback) ->
    read @_recordFile, Buffer(@_size), 0, @_size, 0, (..., buffer) =>
      @_freeslotLst = @_unpack buffer
      @_blklenMap = @_createFreemap()
      callback()

  _createFreemap: ->
    freemap = {}
    for {fragment}, num in @_freeslotLst when fragment?
      freemap[fragment] ?= []
      freemap[fragment].push num

    freemap

  _unpack: (buffer) ->
    freelst = []
    curr = prev = next = null

    if buffer.length is Hive::UNITSIZE
      curr = buffer.readUInt32BE 0
      freelst[curr] = {prev, next, len: buffer[-1..],readUInt8 0}
      return freelst

    for unit, i in buffer[...-Hive::UNITSIZE] by Hive::UNITSIZE
      next = buffer[i + Hive::UNITSIZE].readUInt32BE 0
      curr = unit.readUInt32BE 0
      freelst[curr] = {prev, next, len: unit[-1..].readUInt8 0}
      prev = curr

    curr = buffer[Hive::UNITSIZE..].readUInt32BE 0
    next = null
    freelst[curr] = {prev, next, len: buffer[-1..].readUInt8 0}
    freelst

  _pack: (buffer, num, fragment) ->
    buffer.writeUInt32BE num
    buffer[-1..][0] = fragment

  _levellstAppend: (level, elem) ->
    @_blklenMap[level] ?= []
    @_blklenMap[level].push elem

  _cellBlklen: (size) ->
    Math.ceil size // Slot::BLKSIZE

  _mergeFreeblk: (seek, fragment) ->
    # location point
    curr = seek // Slot::BLKSIZE
    next = pnext = null
    prev = nprev = null
    nnext = null
    pprev = null

    # fragment length
    len = fragment
    nlen = NaN
    plen = NaN

    # append first free slot
    if @_freeslotLst.length is 0
      @_freeslotLst[curr] = {prev, next, len}
      @_levellstAppend len, curr
      return

    # calculate the location points
    sublst = @_freeslotLst[curr..]
    if sublst.length > 0
      for item, i in sublst when item?
        {prev: nprev, next: nnext, len: nlen} = item
        prev = nprev
        next = ncurr = i + curr
        prevItem = @_freeslotLst[prev]
        {prev: pprev, len: plen} = prevItem if prevItem?
        break
    else
      for i in [curr..0] when @_freeslotLst[i]?
        {prev: pprev, len: plen} = @_freeslotLst[i]
        prev = pcurr = i
        break

    if prev + plen is curr
      item = @_freeslotLst[prev]
      item.len += len
      unorderlstRm @_blklenMap[plen], prev
      if curr + len is next
        # merge with prev & next
        item.next = nnext
        item.len += nlen
        @_levellstAppend item.len, prev

        delete @_freeslotLst[next]
        unorderlstRm @_blklenMap[nlen], next
      else
        # merge with prev
        @_levellstAppend item.len, prev
    else
      item = @_freeslotLst[curr] = {prev, next, len}
      @_freeslotLst[prev]?.next = curr
      @_freeslotLst[next]?.prev = curr
      if curr + len is next
        # merge with next
        item.next = nnext
        item.len += nlen
        @_levellstAppend item.len, curr

        delete @_freeslotLst[next]
        unorderlstRm @_blklenMap[nlen], next

  _getAvailiableSeek: (fragment) ->
    for levellst in @_blklenMap[fragment..] when freelst?.length
      curr = levellst.pop()
      {prev, next, len} = @_freeslotLst[curr]
      delete @_freeslotLst[curr]
      diff = len - fragment
      if diff > 0
        newcurr = curr + fragment
        @_freeslotLst[newcurr] = {prev, next, len: diff}
        @_freeslotLst[prev].next = @_freeslotLst[next].prev = newcurr
        @_levellstAppend diff, newcurr
      else
        @_freeslotLst[prev].next = next
        @_freeslotLst[next].prev = prev
      return curr * Slot::BLKSIZE

    @slot.size

  alloc: (data, callback) ->
    {idx} = data
    cell = @slot.alloc data
    blklen = @_cellBlklen cell.size
    seek = @_getAvailiableSeek blklen
    @index.ensure idx, seek, blklen, (err) =>
      @slot.push seek, cell, callback

  seek: (idx, callback) ->
    @index.seekfor idx, (err, info) =>
      {seek, fragment} = info
      @slot.skipto seek, fragment, callback

  rewrite: (idx, data, callback) ->
    cell = @slot.alloc data
    blklen = @_cellBlklen cell.size
    @index.seekfor idx, (err, info) =>
      {seek, fragment} = info
      return callback new Error "Index not found" unless seek?
      if blklen > fragment
        # realloc
        seek = @_getAvailiableSeek blklen
        @index.update idx, seek, blklen, (err) =>
          @slot.push seek, cell, callback
      else
        @slot.push seek, cell, callback

  free: (idx, callback) ->
    @index.seekfor idx, (err, info) =>
      {seek, fragment} = info
      return callback null unless seek?
      @_mergeFreeblk seek, fragment
      @index.drop idx, callback

  _close: (callback) -> close @_recordFile, callback

  close: (callback) ->
    @index.close => @_close => @slot.close =>
      @_closed = yes
      callback()

module.exports = (args, callback) ->
  new Hive args
  .init callback

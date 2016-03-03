{openSync, write, read, fstatSync, close} = require 'fs'
Index = require './seek'
Slot = require './slot'

class Hive

  UNITSIZE: 5

  constructor: (args) ->
    @_recordFile = openSync args.file, 'a+'
    @index = new Index args.indexcfg
    @slot = new Slot args.slotcfg

  init: (callback) ->
    @index.init => @slot.init => @_initFreeblk => callback this

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

  _pack: (num, fragment) ->
    buffer = Buffer UNITSIZE
    buffer.writeUInt32BE num
    buffer[-1..].writeUInt8 fragment

  _mergeFreeblk: (seek, fragment) ->
    curr = seek // Slot::BLKSIZE
    if @_freeslotLst.length is 0
      @_freeslotLst[curr] = prev: null, next: null, len: fragment
      @_blklenMap[fragment] ?= []
      @_blklenMap[fragment].push curr
      return

    for item, num in @_freeslotLst[curr..] when item?
      {prev: nprev, next: nnext, len: nlen} = item
      ncurr = num + curr
      break

    unless ncurr?
      for num in [curr..0] when item = @_freeslotLst[num]?
        {prev: pprev, next: pnext, len: plen} = item
        pcurr = num
        break

      if pcurr + plen is curr
        newlen = plen + fragment
        @_freeslotLst[pcurr].len = newlen
        @_blklenMap[newlen] ?= []
        @_blklenMap[newlen].push pcurr

        oldlevellst = @_blklenMap[plen]
        pInmap = oldlevellst.indexOf pcurr
        if pInmap is oldlevellst.length - 1
          oldlevellst.pop()
        else
          oldlevellst[pInmap] = oldlevellst.pop()
      else
        @_freeslotLst[curr] = prev: pcurr, next: pnext, len: fragment
        @_freeslotLst[pcurr].next = curr
    else
      nprevItem = @_freeslotLst[nprev]
      if curr + fragment is ncurr
        delete @_freeslotLst[ncurr]

        oldnlevellst = @_blklenMap[nlen]
        nInmap = oldnlevellst.indexOf ncurr
        if nInmap is oldnlevellst.length - 1
          oldnlevellst.pop()
        else
          oldnlevellst[nInmap] = oldnlevellst.pop()

        if nprevItem? and nprev + nprevItem.len is curr
          {len: plen} = nprevItem
          newlen = plen + fragment + nlen
          @_freeslotLst[nprev] = prev: nprev, next: nnext, len: newlen
          @_blklenMap[newlen] ?= []
          @_blklenMap[newlen].push nprev

          oldplevellst = @_blklenMap[plen]
          pInmap = oldplevellst.indexOf nprev
          if pInmap is oldplevellst.length - 1
            oldplevellst.pop()
          else
            oldplevellst[pInmap] = oldplevellst.pop()
        else
          newlen = fragment + nlen
          @_freeslotLst[curr] = prev: nprev, next: nnext, len: newlen
      else if nprevItem? and nprev + nprevItem.len is curr
        {len: plen} = nprevItem
        newlen = plen + fragment
        @_freeslotLst[nprev].len = newlen

        oldplevellst = @_blklenMap[plen]
        pInmap = oldplevellst.indexOf nprev
        if pInmap is oldplevellst.length - 1
          oldplevellst.pop()
        else
          oldplevellst[pInmap] = oldplevellst.pop()
      else
        @_freeslotLst[curr] = prev: nprev, next: ncurr, len: fragment
        @_freeslotLst[nprev].next = @_freeslotLst[ncurr].prev = curr
        @_blklenMap[fragment] ?= []
        @_blklenMap[fragment].push curr

  _getAvailiableSeek: (len) ->
    for levellst in @_blklenMap[len..] when freelst?.length
      curr = levellst.pop()
      {prev, next, len: alen} = @_freeslotLst[curr]
      delete @_freeslotLst[curr]
      diff = alen - len
      if diff > 0
        newcurr = curr + len
        @_freeslotLst[newcurr] = {prev, next, len: diff}
        @_freeslotLst[prev].next = @_freeslotLst[next].prev = newcurr
        @_blklenMap[diff] ?= []
        @_blklenMap[diff].push newcurr
      else
        @_freeslotLst[prev].next = next
        @_freeslotLst[next].prev = prev
      return curr

    @slot.size

  alloc: (data, callback) ->
    {idx} = data
    cell = @slot.alloc data
    seek = @_getAvailiableSeek cell.blklen
    @index.ensure idx, seek, cell.blklen, (err) =>
      @slot.push seek, cell, callback

  seek: (idx, callback) ->
    @index.seekfor idx, (err, info) =>
      {seek, fragment} = info
      @slot.skipto seek, fragment, callback

  rewrite: (idx, data, callback) ->
    cell = @slot.alloc data
    @index.seekfor idx, (err, info) =>
      {seek, fragment} = info
      return callback new Error "Index not found" unless seek?
      if cell.blklen > fragment
        # realloc
        seek = @_getAvailiableSeek cell.blklen
        @index.update idx, seek, cell.blklen, (err) =>
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
    @index.close => @_close => @slot.close => callback()

module.exports = (args, callback) ->
  new Hive args
  .init callback

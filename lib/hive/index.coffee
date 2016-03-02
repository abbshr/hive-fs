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
      @_freelst = @_unpack buffer
      @_freemap = @_createFreemap()
      callback()

  _createFreemap: ->
    freemap = {}
    for {fragment}, num in @_freelst when fragment?
      freemap[fragment] ?= []
      freemap[fragment].push num

    freemap

  _unpack: (buffer) ->
    freelst = []
    prev = next = null

    for unit, i in buffer by Hive::UNITSIZE
      next = if i + Hive::UNITSIZE is buffer.length
        null
      else
        buffer[i + Hive::UNITSIZE..].readUInt32BE 0
      curr = unit.readUInt32BE 0
      freelst[curr] =
        {prev, next, fragment: unit[-1..].readUInt8 0}
      prev = curr

    freelst

  _pack: (num, fragment) ->
    buffer = Buffer(UNITSIZE)
    buffer.writeUInt32BE num
    buffer[-1..].writeUInt8 fragment

  _mergeFreeblk: (seek, fragment) ->
    num = seek // Slot::BLKSIZE
    for info, _num in @_freelst[num..] when info?
      next_fragment = info.fragment
      {prev, next} = info
      break

    # TODO:
    # fix bug in unordered list delete algorithm O(1)
    if num + fragment is _num
      prev_fragment = @_freelst[prev]
      if prev + @_freelst[prev].fragment is num
        @_freelst[prev] =
          fragment: prev_fragment + fragment + next_fragment
          prev: @_freelst[prev].prev
          next: next
        delete @_freelst[_num]
        @_freemap[@_freelst[prev].fragment] ?= []
        @_freemap[@_freelst[prev].fragment].push prev
        prevIdx = @_freemap[prev_fragment].indexOf prev
        @_freemap[prev_fragment][prevIdx] = @_freemap[prev_fragment].pop()
        nextIdx = @_freemap[next_fragment].indexOf _num
        @_freemap[next_fragment][nextIdx] = @_freemap[next_fragment].pop()
      else
        @_freelst[num] =
          fragment: fragment + next_fragment
          prev: prev
          next: next
        delete @_freelst[_num]
        @_freemap[@_freelst[num].fragment] ?= []
        @_freemap[@_freelst[num].fragment].push num
        nextIdx = @_freemap[next_fragment].indexOf _num
        @_freemap[next_fragment][nextIdx] = @_freemap[next_fragment].pop()
    else if prev + @_freelst[prev].fragment is num
      @_freelst[prev] =
        fragment: prev_fragment + fragment
        prev: @_freelst[prev].prev
        next: _num
      @_freemap[@_freelst[prev].fragment] ?= []
      @_freemap[@_freelst[prev].fragment].push prev
      prevIdx = @_freemap[prev_fragment].indexOf prev
      @_freemap[prev_fragment][prevIdx] = @_freemap[prev_fragment].pop()
    else
      @_freelst[num] =
        fragment: fragment
        prev: prev
        next: _num
      @_freemap[fragment] ?= []
      @_freemap[fragment].push num

  _getAvailiableSeek: (len) ->
    freelst = @_freemap[len..][0]
    if freelst?.length > 0
      num = freelst.pop()
      {prev, next, fragment} = @_freelst[num]
      diff = fragment - num - len
      if diff > 0
        @_freelst[prev].next = @_freelst[next].prev = num + len
        @_freelst[num + len] = {prev, next, fragment: diff}
        @_freemap[diff] ?= []
        @_freemap[diff].push num + len
      else
        @_freelst[prev].next = next
        @_freelst[next].prev = prev

      delete @_freelst[num]
    else
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

{openSync, write, read, fstatSync, close} = require 'fs'
Cell = require '../cell'

class Slot

  BLKSIZE: 2 + 2 ** 16 # 64KB per slot

  constructor: (args) ->
    @_flag = if args.producer then 'a+' else 'r'
    @_file = args.file

  flushSize: ->
    {size} = fstatSync @_slotFile
    size

  init: (callback) ->
    @_slotFile = openSync @_file, @_flag
    @size = @flushSize()

  alloc: (data) ->
    if @_flag is 'r'
      throw new Error "Can not allocate memory for new Slot in Consumer mode"
    else
      cell = new Cell data

  push: (seek, cell, callback) ->
    if @_flag is 'r'
      throw new Error "Can not setup Slot in Consumer mode"
    else
      write @_slotFile
        , cell.buffer
        , 0
        , cell.size, seek, (err, byte, buffer) =>
          @size += byte if seek is @size
          callback err

  skipto: (seek, fragment, callback) ->
    read @_slotFile
      , Buffer(Slot::BLKSIZE * fragment)
      , 0
      , Slot::BLKSIZE * fragment, seek, (err, byte, buffer) =>
        cell = Cell.from buffer
        callback err, cell

  close: (callback = ->) ->
    close @_slotFile, callback

module.exports = Slot

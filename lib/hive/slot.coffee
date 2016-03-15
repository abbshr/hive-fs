{openSync, write, read, fstatSync, close} = require 'fs'
Cell = require '../cell'
{BLK_SIZE} = require '../constants'

class Slot

  constructor: (args) ->
    flag = if args.producer then 'a+' else 'r'
    @_slotFile = openSync args.file, flag
    @size = @updateSize()

  updateSize: ->
    {size} = fstatSync @_slotFile
    size

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
          # console.log @size, byte, seek
          @size += byte if seek is @size
          callback err

  skipto: (seek, len, callback) ->
    size = BLK_SIZE * len
    read @_slotFile
      , Buffer(size)
      , 0
      , size, seek, (err, byte, buffer) =>
        cell = Cell.from buffer
        callback err, cell

  close: (callback = ->) ->
    close @_slotFile, callback

module.exports = Slot

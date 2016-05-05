{openSync, write, read, fstatSync, close} = require 'fs'
Chunk = require './chunk'
{BLK_SIZE} = require './constants'

class Slot

  constructor: (args) ->
    @_slotFile = openSync args.file, "a+"
    @size = @updateSize()

  updateSize: ->
    {size} = fstatSync @_slotFile
    size

  alloc: (data) ->
    Chunk::deflate data
  
  incSize: (seek, size) ->
    @size += size if seek is @size

  push: (seek, {buffer: {head, payload}, size}, callback) ->
    return if @_closed
    # @size += size if seek is @size
    write @_slotFile
      , head
      , 0
      , head.length, seek, (err, byte, buffer) =>
        write @_slotFile
          , payload
          , 0
          , payload.length, seek + byte, (err) ->
            callback err
        
  skipto: (seek, len, callback) ->
    return if @_closed
    size = BLK_SIZE * len
    read @_slotFile
      , Buffer(size)
      , 0
      , size, seek, (err, byte, buffer) ->
        # console.log buffer.length, buffer
        Chunk::inflate buffer, (data) ->
          callback err, data

  close: (callback = ->) ->
    return if @_closed
    @_closed = yes
    close @_slotFile, (err) ->
      callback err

module.exports = Slot

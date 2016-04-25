{BYTE_SIZE, BLK_SIZE, NULL} = require '../constants'

class Cell

  # KEYSIZE: 32
  # BYTE_SIZE: 4

  # IDXSIZE: 256
  # NULL: 0x00

  constructor: (@data, @buffer) ->
    {@idx} = @data
    throw new Error "Index not found" unless @idx
    @_parse() unless @buffer?

  _parse: ->
    # idx = Buffer Cell::IDXSIZE
    # idx.fill Cell::NULL
    # idx.utf8Write @data.idx
    payload = JSON.stringify @data
    payloadLen = Buffer.byteLength payload, 'utf-8'
    size = payloadLen + BYTE_SIZE
    @blklen = Math.ceil size / BLK_SIZE
    @size = @blklen * BLK_SIZE
    @buffer = Buffer @size

    @buffer.writeUInt32BE payloadLen
    @buffer.write payload, BYTE_SIZE
    @buffer.fill NULL, size
    # buffer = []
    # for key, value of @data
    #   head = Buffer 36
    #   head.fill Cell::NULL
    #   head[0...32].utf8Write key
    #   value = Buffer value
    #   head[-4..].writeUInt32BE value.length
    #   buffer.push head, value
    #
    # @buffer = Buffer.concat [idx, buffer...]

  @from: (buffer) ->
    # size = buffer.length
    # cursor = 0
    # data = {}
    #
    # idx = buffer[cursor ... (cursor = cursor + Cell::IDXSIZE)]
    # nullbyte = idx.indexOf Cell::NULL
    # idx = idx[0...nullbyte] if !!~nullbyte
    # data.idx = idx.toString 'utf-8'
    payloadLen = buffer.readUInt32BE 0
    data = JSON.parse buffer.toString 'utf-8', BYTE_SIZE, BYTE_SIZE + payloadLen

    # while cursor < size
    #   key = buffer[cursor ... (cursor = cursor + Cell::KEYSIZE)].toString 'utf-8'
    #   byte = buffer[cursor ... (cursor = cursor + Cell::BYTE_SIZE)].readUInt32BE 0
    #   value = buffer[cursor ... (cursor = cursor + byte)].toString 'utf-8'
    #   try
    #     data[key] = JSON.parse value
    #   catch err
    #     console.error err

    cell = new Cell data, buffer
    cell.size = buffer.length
    cell.blklen = buffer.length // BLK_SIZE
    cell

module.exports = Cell

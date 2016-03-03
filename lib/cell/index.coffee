
class Cell

  KEYSIZE: 32
  BYTESIZE: 4
  IDXSIZE: 256

  constructor: (@data, @buffer) ->
    {@idx} = @data
    throw new Error "Index not found" unless @idx
    @_parse() unless @buffer?

  _parse: ->
    idx = Buffer Cell.IDXSIZE
    idx.utf8Write @data.idx

    buffer = for key, value of @data
      buffer = Buffer 36
      buffer.utf8Write key
      value = Buffer value
      buffer[-4..].writeUInt32BE value.length
      Buffer.concat [buffer, value]

    @buffer = Buffer.concat [idx, buffer...]
    @size = @buffer.length

  @from: (buffer) ->
    size = buffer.length
    cursor = 0
    blklen = blk
    data = {}

    data.idx = buffer[cursor ... (cursor = cursor + Cell.IDXSIZE)].toString 'utf-8'
    while cursor < size
      key = buffer[cursor ... (cursor = cursor + Cell.KEYSIZE)].toString 'utf-8'
      byte = buffer[cursor ... (cursor = cursor + Cell.BYTESIZE)].readUInt32BE 0
      value = buffer[cursor ... (cursor = cursor + byte)].toString 'utf-8'
      try
        data[key] = JSON.parse value
      catch err
        console.error err

    cell = new Cell data, buffer
    cell.size = size

module.exports = Cell

{BYTE_SIZE, BLK_SIZE, NULL} = require './constants'
cbor = require 'cbor'

class Chunk

  deflate: (data) ->
    payload = cbor.encode data
    payload_size = payload.length
    r_size = payload_size + BYTE_SIZE
    blklen = Math.ceil r_size / BLK_SIZE
    size = blklen * BLK_SIZE
    head = Buffer BYTE_SIZE
    head.writeUInt32BE payload_size
    
    {buffer: {head, payload}, blklen, size}

  inflate: (raw, callback) ->
    payload_size = raw.readUInt32BE 0
    # console.log payload_size
    r_size = payload_size + BYTE_SIZE
    cbor.decodeFirst raw[BYTE_SIZE ... r_size], (err, data) ->
      # console.log err
      callback data

module.exports = Chunk

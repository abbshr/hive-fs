module.exports =
  # 记录单个chunk的大小
  BYTE_SIZE: 4
  # 记录slot的起始位置
  SEEK_SIZE: 4
  # 记录一个chunk中包含的连续slot数
  FRAGMENTLEN_SIZE: 1  
  # TIMESTAMP_SIZE: 8
  # key的最大尺寸
  IDX_SIZE: 32
  # slot的固定大小
  BLK_SIZE: 20
  # free记录大小
  UNIT_SIZE: 8
  # flag
  NULL: 0x00
  FREE: 0x00
  USED: 0x01
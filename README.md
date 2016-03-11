H.I.V.E - fs
------------

hive-fs 是一个用 CoffeeScript 写的 **基于索引和偏移量** 的文件系统.

原本是针对我的毕业设计 Archangel 而设计实现的, Leviathan 内部组件之一, 用于在多个 Nero-Kodiak 集群节点之间/内部同步 Gateway 的核心数据块(配置信息), 也同样适用于一些其他基础层面的同步服务.

## 架构

### Slot 索引

### Tuple 结构

### Cell 结构

### 块缓冲 & 回写

### 资源管理

#### 空闲表

#### Slot 回收与重用

#### 分配不定长 Cell

#### 资源限制(参数待调优)
`lib/constants.coffee`:

```yaml
# cell
# KEYSIZE: 32B # Cell 中每个 key 的最大字节长度, ('idx' 不存储)
BYTESIZE: 4B # 存储 Cell 中每个 value 的最大字节长度, 即标准 4 字节整型长度
# freelst
UNITSIZE: 5B # 空闲表中每条记录的最大字节长度, 包括: 4 字节的整数表示空闲 slot 的起始 seek 位置, 1 字节的整数表示连续块数量
# seek index
TUPLESIZE: 270B # seek-index 表中每条记录的最大字节长度, 包括: 1 字节的整数表示该 tuple 是否空闲, 4 字节的整数表示某个 Cell 的起始 seek 位置, 1 字节的整数表示连续块数量, 8 字节存储时间戳, 剩余 256 字节是索引占用的最大字节长度.
BLKSIZE: 1KB # 每个 Slot 块的标准大小
```

## 示例 && API

```coffee
{Hive} = require './'

# Hive 配置(可选)
cfg =
  rw: yes
  dirname: '/dev/shm'
  basename: 'hive-fs'

data =
  idx: 'github:abbshr' # 必须字段
  ts: Date.now() # 必须字段
  name: 'Ran Aizen'
  id: 1237104
  activeDay: [1,3,5,7,9]
  following: ['@_the_flash', '@_batman', '@_the_arrow', '@_superman']

hive = new Hive cfg
# 申请 slots, 并构造一个 cell 来写数据
hive.write data, (err) ->
# 跳至索引记录的 slot 偏移量, 从 buffer 构建 cell
hive.seek 'github:abbshr', (err, cell) ->
# 释放一个 cell
hive.free 'github:abbshr', (err) ->
# 关闭 hive
hive.close (err) ->
```

建议: 配合共享内存以进一步提升性能.

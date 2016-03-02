[WIP] H.I.V.E - fs
------------

(尚未测试, 优化重构中)

hive-fs 是一个用 CoffeeScript 写的 **基于索引和偏移量** 的文件系统.

原本是针对我的毕业设计 Archangel 而设计实现的, Leviathan 内部组件之一, 用于在多个 Nero-Kodiak 集群节点之间/内部同步 Gateway 的核心数据块(配置信息), 也同样适用于一些其他基础层面的同步服务.

# TODO:

## 架构

### Slot 索引

### Tuple 结构

### Cell 结构

### 资源管理

#### 空闲表

#### Cell 回收与重用

#### 不定长分配 Slot

## 示例 && API

```coffee
{Hive} = require './'

# Hive 配置
cfg =
  file: '/tmp/hive.mgr'
  indexcfg:
    file: '/tmp/index-seek.mgr'
    producer: yes
  slotcfg:
    file: '/tmp/slot'
    producer: yes

Hive cfg, (hive) ->
  # 申请
  hive.alloc
    , idx: 'aws:jack.contacts.Lee'
    , name: 'Lee'
    , number: 110119120
  , (err) ->

  # 更新一个 Slot
  hive.rewrite 'aws:jack.contacts.Lee'
    , name: 'Lee', number: 12345678
  , (err) ->

  # 取一个 Slot 并放入 Cell 中
  hive.seek 'aws:jack.contacts.Lee', (err, cell) ->

  # 释放一个 Slot
  hive.free 'aws:jack.contacts.Lee', (err) ->

  # 关闭 hive
  hive.close ->
```

建议: 配合共享内存以进一步提升性能.

# LSM-KV-STORE
基于LSM的KV存储引擎，充分利用顺序写的思想使数据的存储效率提高而牺牲部分读取效率。

## 主要工作
* 实现 LSM 架构，内存 + 磁盘协同存储，内存数据保存至 Skip List，持久化数据使用 SSTable 磁盘文件存储。
* 多层级 SSTable 设计解决 SSTable 文件过多导致存储效率和查询性能低下的问题。
* SSTable 数据压缩存储，提高存储效率。
* WAL Log 记录写入操作记录，支持机器重启后快速恢复。
* 对 Key 值不重复的高层 SSTable 的查找进行算法优化。
* 实现数据的 dump 和 compaction 操作。

## 测试成果
在 8 核 16 G 的环境下测试，该引擎存储效率较 MySQL 提升了 29.04%。（数据量不大）

## 实现语言及知识点
<img src="https://img.shields.io/badge/Java-100%25-yellowgreen" /> <img src="https://img.shields.io/badge/%E6%9E%B6%E6%9E%84-LSM-orange" /> <img src="https://img.shields.io/badge/%E5%86%85%E5%AD%98-Skip%20List-important" /> <img src="https://img.shields.io/badge/%E7%A3%81%E7%9B%98-SSTable-yellow" />


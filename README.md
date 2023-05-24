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

## 项目结构
* [constant](src%2Fmain%2Fjava%2Fcom%2Fcqupt%2Fkvstore%2Fconstant) 包下是一些抽离出来的可配置常量，便于维护。
* [model](src%2Fmain%2Fjava%2Fcom%2Fcqupt%2Fkvstore%2Fmodel) 包下的 [commond](src%2Fmain%2Fjava%2Fcom%2Fcqupt%2Fkvstore%2Fmodel%2Fcommond) 包中是几种不同的命令对象，其中定义 [Command.java](src%2Fmain%2Fjava%2Fcom%2Fcqupt%2Fkvstore%2Fmodel%2Fcommond%2FCommand.java) 来规范命令对象的行为， [AbstractCommand.java](src%2Fmain%2Fjava%2Fcom%2Fcqupt%2Fkvstore%2Fmodel%2Fcommond%2FAbstractCommand.java) 为了方便复用，在这种追加写模式的存储引擎中，set 操作可以实现增、改。
* [sstable](src%2Fmain%2Fjava%2Fcom%2Fcqupt%2Fkvstore%2Fmodel%2Fsstable) 包下是维护SSTable信息的类，其中 [SSTable.java](src%2Fmain%2Fjava%2Fcom%2Fcqupt%2Fkvstore%2Fmodel%2Fsstable%2FSSTable.java) 用于初始化 SSTable，最为重要。 [SSTableFileMetaInfo.java](src%2Fmain%2Fjava%2Fcom%2Fcqupt%2Fkvstore%2Fmodel%2Fsstable%2FSSTableFileMetaInfo.java) 用于将一个SSTable文件的全部信息收集完毕以后写入磁盘。

## 核心方法介绍
1、SSTable 中 initFromIndex() 方法用于将内存中的数据（跳表）持久化到磁盘并在磁盘中存储数据、索引、元信息。索引物理上使用的是稀疏索引，数据结构是红黑树。
```java
    /**
     * 将跳表正式转成sstable
     */
    private void initFromIndex(ConcurrentSkipListMap<String,Command> index){
        try {
            JSONObject partData = new JSONObject(true);
            tableMetaInfo.setDataStart(tableFile.getFilePointer());
            for (Command command : index.values()) {
                if(command instanceof SetCommand){
                    SetCommand set = (SetCommand) command;
                    partData.put(set.getKey(),set);
                }

                if(command instanceof RmCommand){
                    RmCommand rm = (RmCommand) command;
                    partData.put(rm.getKey(),rm);
                }

                // 达到分段数量，开始写入数据
                if(partData.size() >= tableMetaInfo.getPartSize()){
                    writeDataPart(partData);
                }
            }
            // 遍历完以后如果有剩余数据，（没有达到分段而剩余的）
            if(partData.size() > 0){
                writeDataPart(partData);
            }
            long dataPartLen = tableFile.getFilePointer() - tableMetaInfo.getDataStart();
            tableMetaInfo.setDataLen(dataPartLen);

            //保存稀疏索引
            byte[] indexBytes = JSONObject.toJSONString(sparseIndex).getBytes(StandardCharsets.UTF_8);
            tableMetaInfo.setIndexStart(tableFile.getFilePointer());
            tableFile.write(indexBytes);
            tableMetaInfo.setIndexLen(indexBytes.length);
            tableMetaInfo.setSmallestKey(index.firstKey());
            tableMetaInfo.setLargestKey(index.lastKey());
            tableMetaInfo.setFileSize(dataPartLen);

            // 该方法一调用就将上面的元信息写入tableFile指向的这个文件了，于是这个文件就有了数据、TreeMap的索引，元信息，并且key还是有序的
            tableMetaInfo.writeToFile(tableFile);
        }catch (Throwable t){
            throw new RuntimeException(t);
        }
    }

    private void writeDataPart(JSONObject partData) throws IOException {
        // 获取到字节流
        byte[] partDataBytes = partData.toJSONString().getBytes(StandardCharsets.UTF_8);
        if(enablePartDataCompress){
            partDataBytes = Snappy.compress(partDataBytes);
        }
        // 获取文件当前的偏移量位置
        long start = tableFile.getFilePointer();
        tableFile.write(partDataBytes);

        //记录数据段的第一个key到稀疏索引中,JSONObject本就是一种JSON类型的Map数据结构
        Optional<String> firstKey = partData.keySet().stream().findFirst();
        // 如果第一个key不为空，那就存入稀疏索引
        byte[] finalPartDataBytes = partDataBytes;
        /**
         * 索引的数据结构就是 key : 这一段数据的位置
         * 位置又是一个对象，封装了起始的偏移量和这一段数据的长度
         * 一种稀疏索引，数据结构的话是红黑树
         * 查找的过程就变成了，打开一个sstable之后先二分查找key，完了通过红黑树Ologn的复杂度找到具体的位置，在这个位置不是直接拿到数据，因为是稀疏索引，需要再遍历
         * 为什么用TreeMap而不是hashMap
         */
        firstKey.ifPresent(key -> sparseIndex.put(key,new Position(start,finalPartDataBytes.length)));
        partData.clear();
    }
```

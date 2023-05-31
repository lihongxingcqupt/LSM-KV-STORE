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
<img src="https://img.shields.io/badge/%E7%B4%A2%E5%BC%95-%E7%BA%A2%E9%BB%91%E6%A0%91%E7%BB%93%E6%9E%84%EF%BC%8C%E7%A8%80%E7%96%8F%E7%B4%A2%E5%BC%95%E5%AD%98%E5%82%A8-blue" />
<img src="https://img.shields.io/badge/%E6%95%B0%E6%8D%AE%E5%8E%8B%E7%BC%A9-snappy-brightgreen" />

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
2、通过ssTable根据key来找值，先遍历当前sstable的索引，当前索引不同的是它是稀疏索引，存的是那一块的第一个key，因此只要当前要找的key要小就有可能
因此，在一个sstable内部来查找数据就是遍历索引中的key，当前key小于目标key就将这个key对应的position放入集合，完了遍历集合，通过position去拿到这段
JSON，完了判断当前的key在不在即可。
```
public Command query(String key) {
        try {
            LinkedList<Position> sparseKeyPositionList = new LinkedList<>();
            //从稀疏索引中找到最后一个小于key的位置，以及第一个大于key的位置
            for (String k : sparseIndex.keySet()) {
                if (k.compareTo(key) <= 0) {
                    sparseKeyPositionList.add(sparseIndex.get(k));
                } else {
                    break;
                }
            }
            if (sparseKeyPositionList.size() == 0) {
                return null;
            }
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][sparseKeyPositionList]: {}", sparseKeyPositionList);
            //TODO 不同dataPart并不是按顺序存在内存上的呀？（但是不同dataPart之间的数据是按顺序的）
            //读取数据块的内容
            for (Position position : sparseKeyPositionList) {
                JSONObject dataPartJson = BlockUtils.readJsonObject(position, enablePartDataCompress, tableFile);
                LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][dataPartJson]: {}", dataPartJson);
                if (dataPartJson.containsKey(key)) {
                    JSONObject value = dataPartJson.getJSONObject(key);
                    return ConvertUtil.jsonToCommand(value);
                }
            }
            return null;
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }
```

3、set的流程
先存内存，内存达到阈值后调用switchIndex将内存冻结、生产walTmp，将数据写入第0层：
```
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            indexLock.writeLock().lock();
            //先保存数据到WAL中
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            memtable.put(key, command);

            //内存表大小超过阈值进行持久化
            if (memtable.size() > storeThreshold) {
                switchIndex();
                dumpToL0SsTable();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

```
在0层的ssTable数量达到一定数值以后再compaction至高层

4、get的实现：
```
 @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            // 先从内存中找
            Command command = memtable.get(key);

            // 内存中找不到从冻结内存中找
            if(command == null && immutableMemtable != null){
                command = immutableMemtable.get(key);
            }

            // 还是没找到就去持久层开始找了
            if(command == null){
                command = findFromSsTable(key);
            }

            // 因为是从后往前找的，假如找到了set，那对应的value就是值
            if(command instanceof SetCommand){
                return ((SetCommand)command).getKey();
            }

            // 假如先找到了删除命令，那就说明这个键被删除了，返回null
            if(command instanceof RmCommand){
                return null;
            }
            // 什么也找不到
            return null;
        }catch (Throwable t){
            throw new RuntimeException(t);
        }

    }
```

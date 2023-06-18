package com.cqupt.kvstore.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cqupt.kvstore.compaction.Compactioner;
import com.cqupt.kvstore.constant.KvConstant;
import com.cqupt.kvstore.model.commond.Command;
import com.cqupt.kvstore.model.commond.RmCommand;
import com.cqupt.kvstore.model.commond.SetCommand;
import com.cqupt.kvstore.model.sstable.SSTable;
import com.cqupt.kvstore.utils.ConvertUtil;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.collections.CollectionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author lihongxing
 * @Date 2023/5/27 22:34
 * 存储引擎的接口实现
 */
public class LsmKvStore implements KvStore {

    /**
     * 日志文件名
     */
    public static final String WAL = "wal";
    /**
     * 读写模式常量
     */
    public static final String RM_MODE = "rw";
    /**
     * 内存数据到达阈值以后将wal转成walTmp
     */
    public static final String WAL_TMP = "walTmp";

    /**
     * 内存数据
     */
    private ConcurrentSkipListMap<String, Command> memtable;

    /**
     * 内存达到阈值以后将其转成这个不可变的内存数据
     */
    private ConcurrentSkipListMap<String, Command> immutableMemtable;

    /**
     * 将整个的存储引擎信息加载到这里，（第几层有哪些ssTable）
     */
    private Map<Integer, List<SSTable>> levelMetaInfos = new ConcurrentHashMap<>();

    /**
     * 工作空间路径
     */
    private final String dataDir;

    /**
     * 读写锁，开始操作时打开，用于防止并发的问题
     */
    private final ReadWriteLock indexLock;

    /**
     * 阈值
     */
    private final int storeThreshold;

    /**
     * 用于实现稀疏索引，当数据的长度达到这个值的时候再将其压缩后写入，索引对应的也就是这一块数据的起始偏移量
     */
    private final int partSize;

    /**
     * 文件句柄，真正用来操作文件的
     */
    private RandomAccessFile wal;

    /**
     * 日志文件对象
     */
    private File walFile;

    /**
     * 自动给文件编号的，每次自动的加一
     */
    private final AtomicLong nextFileNumber = new AtomicLong(1);

    /**
     * 数据量达到一定阈值之后通过compaction写到高层
     */
    private Compactioner compactioner;

    private BloomFilter<CharSequence> bloomFilter;
    public LsmKvStore(String dataDir, int storeThreshold, int partSize,int expectedSize, double falsePositiveRate) {
        try {
            this.dataDir = dataDir;
            this.storeThreshold = storeThreshold;
            this.partSize = partSize;
            this.indexLock = new ReentrantReadWriteLock();
            File dir = new File(dataDir);
            File[] files = dir.listFiles();
            levelMetaInfos = new ConcurrentHashMap<>();
            memtable = new ConcurrentSkipListMap<>();
            walFile = new File(dataDir + WAL);
            wal = new RandomAccessFile(dataDir + WAL, RM_MODE);
            compactioner = new Compactioner();
            this.bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), expectedSize, falsePositiveRate);
            /**
             * 目录是下是空的，不用加载sstable，否则要将sstable加载进来
             */
            if(files == null || files.length == 0){
                return;
            }

            /**
             * 目录不是空的，逐一进行加载，包括加载ssTable、日志
             */
            for(File file : files){
                String fileName = file.getName();
                if(file.isFile() && fileName.equals(WAL_TMP)){
                    /**
                     * 存在暂存日志文件就说明在持久化过程中出了差错
                     * 它就是由wal转变成的，因此它没有被删就是出问题了，要进行操作
                     * 其实就是根据存入时候的数据结果去读取文件的时候将它反回来再放入memtable中就好了
                     */
                    restoreFromWal(new RandomAccessFile(file,RM_MODE));
                }
                /**
                 * 加载ssTable
                 */
                if(file.isFile() && fileName.endsWith(KvConstant.FILE_SUFFIX)){
                    SSTable ssTable = SSTable.createFromFile(file.getAbsolutePath(),true);
                    Integer level = ssTable.getLevel();
                    List<SSTable> temLevelssTables = null;
                    /**
                     * 将ssTable维护到层信息中
                     */
                    if(levelMetaInfos.get(level) == null){
                        temLevelssTables = new ArrayList<>();
                        levelMetaInfos.put(level,temLevelssTables);
                    }else{
                        temLevelssTables = levelMetaInfos.get(level);
                    }
                    temLevelssTables.add(ssTable);
                }
                /**
                 * 加载日志，说明是内存还没来得及持久化到SSTable就宕机了，这个时候日志中的数据和内存是同步的
                 * 复用从暂存日志恢复数据那个方法即可，因为它们存储数据结构是一模一样的
                 */
                if(file.isFile() && fileName.equals(WAL)){
                    walFile = file;
                    wal = new RandomAccessFile(file,RM_MODE);
                    restoreFromWal(wal);
                }
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 从暂存日志中将数据恢复
     * @param wal
     */
    private void restoreFromWal(RandomAccessFile wal) {
        try {
            long len = wal.length();
            long start = 0;
            wal.seek(start);
            while (start < len){
                // 先读取数据大小，这里和存入时的数据结构是对应起来的
                int valueLen = wal.readInt();
                // 根据数据大小读取数据
                byte[] bytes = new byte[valueLen];
                wal.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = ConvertUtil.jsonToCommand(value);
                if(command != null){
                    memtable.put(command.getKey(),command);
                }
                start += 4; // 指出该条数据的那个长度字段的长度
                start += valueLen;// 数据的长度
            }
            wal.seek(wal.length());
        }catch (Throwable t){
            new RuntimeException(t);
        }
    }

    /**
     * 存储数据的逻辑
     * @param key
     * @param value
     */
    @Override
    public void set(String key, String value) {
        try {
            bloomFilter.put(key);
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            indexLock.writeLock().lock();

            //先将数据写入日志，先记录当前数据转成字节流后的长度，读取的时候也是根据长度来读取
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            memtable.put(key,command);

            // 超过阈值，进行持久化
            if(memtable.size() > storeThreshold){
                //冻结内存表和日志
                switchIndex();
                dumpToL0SsTable();
            }
        }catch (Throwable t){
            throw new RuntimeException(t);
        }
    }

    private void dumpToL0SsTable() {
        try {
            long fileNumber = nextFileNumber.getAndIncrement();
            SSTable ssTable = SSTable.createFromIndex(fileNumber,partSize,immutableMemtable,true,0);

            // ssTable信息记录
            List<SSTable> levelSStables = levelMetaInfos.get(0);
            if(levelSStables == null){
                levelSStables = new ArrayList<>();
                levelMetaInfos.put(0,levelSStables);
            }
            // 将层信息维护在内存当中
            levelSStables.add(ssTable);

            // 持久化完成以后删除暂存的内存表和tmpWal
            immutableMemtable = null;
            File tmpWal = new File(dataDir + WAL_TMP);
            if(tmpWal.exists()){
                tmpWal.delete();
            }
            //  可能会触发compaction
            compactioner.compaction(levelMetaInfos,nextFileNumber);
        }catch (Throwable t){
            throw new RuntimeException(t);
        }
    }

    /**
     * 将内存表和日志都切换成不可变那种，清空自己以后继续去接收新的写请求
     */
    private void switchIndex() {
        try{
            indexLock.writeLock().lock();
            //内存表冻结
            immutableMemtable = memtable;
            memtable = new ConcurrentSkipListMap<>();
            wal.close();

            //切换WAL
            File tmpWal = new File(dataDir + WAL_TMP);
            if(tmpWal.exists()){
                tmpWal.delete();
            }
            // 将日志直接reName以后其实就是将它转成了冻结日志
            walFile.renameTo(tmpWal);

            walFile = new File(dataDir + WAL);
            wal = new RandomAccessFile(walFile,RM_MODE);
        }catch (Throwable t){
            throw new RuntimeException(t);
        }finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public String get(String key) {
        try {
            /**
             * 检查布隆过滤器，假如返回false说明不在存储引擎里面，直接返回null，避免了不断去找数据的开销
             */
            if (!bloomFilter.mightContain(key)) {
                return null;
            }
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

    /**
     * 内存中没找到就来ssTable中也就是磁盘中开始找
     * @param key
     * @return
     */
    private Command findFromSsTable(String key) {
        // 1、查找level0
        Command l0Result = findFromL0SsTable(key);
        if(l0Result != null){
            return l0Result;
        }

        // 2、第一层没找到，下沉，往下找
        for(int level = 1;level < KvConstant.SSTABLE_MAX_LEVEL;++level){
            Command otherLevelResult = findFromOtherLevelSsTable(key,level);
            if(otherLevelResult != null){
                return otherLevelResult;
            }
        }
        return null;
    }



    /**
     * 从第0层找数据，先从levelInfo中将对应层的全部ssTable集合拿出来后将其按编号从大到小排序以后开始遍历
     * @param key
     * @return
     */
    private Command findFromL0SsTable(String key) {
        List<SSTable> l0SsTables = levelMetaInfos.get(0);
        if(CollectionUtils.isEmpty(l0SsTables)){
            return null;
        }

        // 将该层的sstable按编号从大到小排序，找的话就是从大到小来找，一旦找到就返回，这样找到的就是最新的数据
        l0SsTables.sort((sstable1,sstable2)->{
            if(sstable1.getFileNumber() < sstable2.getFileNumber()){
                return 1;
            }else if(sstable1.getFileNumber() == sstable2.getFileNumber()){
                return 0;
            }else{
                return  -1;
            }
        });

        // 存在多个key相同的数据时以最新的为准
        for (SSTable ssTable : l0SsTables){
            Command query = ssTable.query(key);
            if(query != null){
                return query;
            }
        }
        return null;
    }

    /**
     * 从别的层来找，别的层是块间有序的，可以进行二分查找
     * @param key
     * @param level
     * @return
     */
    private Command findFromOtherLevelSsTable(String key, int level) {
        List<SSTable> ssTables = levelMetaInfos.get(level);
        if(CollectionUtils.isEmpty(ssTables)){
            return null;
        }

        // 按照ssTable中的最小key来从小到大排序
        ssTables.sort((sstable1,sstable2) -> {
            if(sstable1.getTableMetaInfo().getSmallestKey().compareTo(sstable2.getTableMetaInfo().getSmallestKey()) < 0){
                return -1;
            } else if (sstable1.getTableMetaInfo().getSmallestKey().compareTo(sstable2.getTableMetaInfo().getSmallestKey()) == 0){
                return 0;
            }else{
                return 1;
            }
        });
        Command command = binarySearchSsTables(key,ssTables);
        if(command != null){
            return command;
        }
        return null;
    }

    /**
     * 高层的块与块之间不存在重复的key了就可以进行二分查找了
     * @param key
     * @param ssTables
     * @return
     */
    private Command binarySearchSsTables(String key, List<SSTable> ssTables) {
        if(ssTables == null){
            return null;
        }
        int left = 0,right = ssTables.size() - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            SSTable midSsTable = ssTables.get(mid);
            if (key.compareTo(midSsTable.getTableMetaInfo().getSmallestKey()) >= 0
                    && key.compareTo(midSsTable.getTableMetaInfo().getLargestKey()) <= 0) {
                return midSsTable.query(key);
            } else if (key.compareTo(midSsTable.getTableMetaInfo().getSmallestKey()) < 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return null;
    }

    /**
     * 删除数据其实和写入是一样的，因为是追加写的方式，从后往前去找，假如找到的是删除，那就说明这个键被删了，就直接返回null
     * @param key
     */
    @Override
    public void rm(String key) {
        try {
            /**
             * 布隆过滤器不支持删除，因为有可能导致别的键出现问题而致使误判
             */
            indexLock.writeLock().lock();
            RmCommand rmCommand = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(rmCommand);
            // 先日志进行记录，防止宕机导致内存数据丢失
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            memtable.put(key,rmCommand);
            if(memtable.size() > storeThreshold){
                switchIndex();
                dumpToL0SsTable();
            }
        }catch (Throwable t){
            throw new RuntimeException(t);
        }finally {
            indexLock.writeLock().unlock();
        }
    }



    /**
     * 将文件流全部关闭
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        wal.close();
        for(List<SSTable> ssTableList : levelMetaInfos.values()){
            ssTableList.forEach(ssTable -> {
                try {
                    ssTable.close();
                }catch (IOException e){
                    throw new RuntimeException(e);
                }
            });
        }
    }



}

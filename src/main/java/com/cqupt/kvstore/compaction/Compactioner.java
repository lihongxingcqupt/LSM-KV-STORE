package com.cqupt.kvstore.compaction;

import com.alibaba.fastjson.JSONObject;
import com.cqupt.kvstore.model.Position;
import com.cqupt.kvstore.model.commond.Command;
import com.cqupt.kvstore.model.sstable.SSTable;
import com.cqupt.kvstore.utils.BlockUtils;
import com.cqupt.kvstore.utils.FileUtils;
import org.apache.commons.collections.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author lihongxing
 * @Date 2023/5/27 22:44
 */
public class Compactioner {
    /**
     * l0中的sstable文件个数超过这个阈值，触发compaction
     */
    private final static Integer L0_DUMP_MAX_FILE_NUM = 3;
    /**
     * 锁
     */
    private final ReentrantLock mutex = new ReentrantLock();

    /**
     * compaction操作，当第0层的文件数量超过阈值，触发
     * @param levelMetaInfos
     * @param nextFileNumber
     */
    public void compaction(Map<Integer, List<SSTable>> levelMetaInfos, AtomicLong nextFileNumber) {
        mutex.lock();
        try {
            doBackgroundCompaction(levelMetaInfos,nextFileNumber);
        }catch (Exception e){
            System.out.println("compaction出错");
        }
    }

    /**
     * compaction的执行
     * @param levelMetaInfos
     * @param nextFileNumber
     */
    private void doBackgroundCompaction(Map<Integer, List<SSTable>> levelMetaInfos, AtomicLong nextFileNumber) throws IOException {
        // 先判断l0是否触发compaction
        if(levelMetaInfos.get(0).size() <= L0_DUMP_MAX_FILE_NUM){
            return;
        }
        // 1、执行l0的compaction，找出和当前新产生的sstable存在重合key的sstable，将他们和下一个level存在key重合的sstable进行合并，并写入下一个level

        // 当前新产生的l0的sstable一定是文件编号最大的
        List<SSTable> l0SSTables = levelMetaInfos.get(0);
        Optional<SSTable> maxFileNumberOptional = l0SSTables.stream().max(Comparator.comparing(SSTable::getFileNumber));
        SSTable lastL0SSTable = maxFileNumberOptional.get();
        List<SSTable> overlappedL0SSTableFileMetaInfos = findOverLapSSTables(lastL0SSTable.getTableMetaInfo().getSmallestKey(), lastL0SSTable.getTableMetaInfo().getLargestKey(), l0SSTables);

        // 计算一批sstable文件覆盖的索引范围
        String smallestKey = null;
        String largestKey = null;
        for (SSTable ssTableFileMetaInfo : overlappedL0SSTableFileMetaInfos) {
            if(smallestKey == null || smallestKey.compareTo(ssTableFileMetaInfo.getTableMetaInfo().getSmallestKey()) > 0){
                smallestKey = ssTableFileMetaInfo.getTableMetaInfo().getSmallestKey();
            }
            if(largestKey == null || largestKey.compareTo(ssTableFileMetaInfo.getTableMetaInfo().getLargestKey()) < 0){
                largestKey = ssTableFileMetaInfo.getTableMetaInfo().getLargestKey();
            }
        }
        // 再获取l1存在重合key的sstable
        List<SSTable> overlappedL1SSTableFileMetaInfos = findOverLapSSTables(smallestKey, largestKey, levelMetaInfos.get(1));

        // 合并成一个文件放到l1
        List<SSTable> overlappedSstables = new ArrayList<>();

        /**
         * 将l0和l1存在key重合的先放到一起
         */
        if(!CollectionUtils.isEmpty(overlappedL0SSTableFileMetaInfos)){
            overlappedSstables.addAll(overlappedL0SSTableFileMetaInfos);
        }

        if (!CollectionUtils.isEmpty(overlappedL1SSTableFileMetaInfos)) {
            overlappedSstables.addAll(overlappedL1SSTableFileMetaInfos);
        }

        /**
         * 按文件编号进行排序
         */
        overlappedSstables.sort((sstable1,sstable2) -> {
            if(sstable1.getFileNumber() < sstable2.getFileNumber()){
                return -1;
            }else if(sstable1.getFileNumber() == sstable2.getFileNumber()){
                return 0;
            }else{
                return 1;
            }
        });

        ConcurrentSkipListMap<String, Command> mergeData = new ConcurrentSkipListMap<>();
        /**
         * 调用此方法相当于根据key来去重，将重复的key去除了
         */
        mergeSSTable(overlappedSstables,mergeData);
        SSTable newSsTable = SSTable.createFromIndex(nextFileNumber.getAndIncrement(), 4, mergeData, true, 1);
        List<SSTable> l1SSTables = levelMetaInfos.get(1);
        if(l1SSTables == null){
            l1SSTables = new ArrayList<>();
            levelMetaInfos.put(1,l1SSTables);
        }
        l1SSTables.add(newSsTable);

        /**
         * 将被合并了的sstable删除，分别将它从该层的sstable集合中删除，还有拿到文件，将该文件删除
         */
        Iterator<SSTable> l0SSTableIterator = l0SSTables.iterator();
        while (l0SSTableIterator.hasNext()){
            SSTable tempSSTable = l0SSTableIterator.next();
            if(containTheSSTable(overlappedL0SSTableFileMetaInfos,tempSSTable.getFileNumber())){
                l0SSTableIterator.remove();
                tempSSTable.close();
                File tmpSSTableFile = new File(FileUtils.buildSStableFilePath(tempSSTable.getFileNumber(), 0));
                tmpSSTableFile.delete();
            }
        }

        Iterator l1SstableIterator = l1SSTables.iterator();
        while (l1SstableIterator.hasNext()) {
            SSTable tempSsTable = (SSTable) l1SstableIterator.next();
            if (containTheSSTable(overlappedL1SSTableFileMetaInfos, tempSsTable.getFileNumber())) {
                l1SstableIterator.remove();
                tempSsTable.close();
                File tmpSstableFile = new File(FileUtils.buildSStableFilePath(tempSsTable.getFileNumber(), 0));
                tmpSstableFile.delete();
            }
        }

    }

    /**
     * 两层重复key的sstable已经准备好了，在这里进行合并
     * @param overlappedSstables
     * @param mergeData
     */
    private void mergeSSTable(List<SSTable> ssTableList, ConcurrentSkipListMap<String, Command> mergeData) {
        ssTableList.forEach(ssTable -> {
            Map<String, JSONObject> jsonObjectMap = readSSTableConten(ssTable);
        });
    }

    /**
     * 将文件中的内容读取到内存
     * @param ssTable
     * @return
     */
    private Map<String, JSONObject> readSSTableConten(SSTable ssTable) {
        HashMap<String, JSONObject> jsonObjectMap = new HashMap<>();
        try {
            TreeMap<String, Position> sparseIndex = ssTable.getSparseIndex();
            for (Position position : sparseIndex.values()) {
                JSONObject jsonObject = BlockUtils.readJsonObject(position, true, ssTable.getTableFile());
                // 遍历每一个key
                for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                    String key = entry.getKey();
                    jsonObjectMap.put(key,jsonObject.getJSONObject(key));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return jsonObjectMap;
    }

    private List<SSTable> findOverLapSSTables(String smallKey,String largestKey,List<SSTable> ssTables){
        List<SSTable> ssTableFileMetaInfos = new ArrayList<>();
        if(ssTables == null){
            return ssTableFileMetaInfos;
        }
        // 遍历该层，找到有重合key的
        for (SSTable ssTable : ssTables) {
            if(!(ssTable.getTableMetaInfo().getLargestKey().compareTo(smallKey) < 0
                    || ssTable.getTableMetaInfo().getSmallestKey().compareTo(largestKey) > 0)){
                ssTableFileMetaInfos.add(ssTable);
            }
        }
        return ssTableFileMetaInfos;
    }

    /**
     * 判断一个sstable列表里面是否包含对应编号的sstable文件
     */
    public boolean containTheSSTable(List<SSTable> ssTables,Long fileNumber){
        for (SSTable ssTable : ssTables) {
            if(ssTable.getFileNumber().equals(fileNumber)){
                return true;
            }
        }
        return false;
    }

    // TODO 加上布隆过滤器
}

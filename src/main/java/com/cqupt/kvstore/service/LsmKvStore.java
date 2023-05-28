package com.cqupt.kvstore.service;

import com.cqupt.kvstore.compaction.Compactioner;
import com.cqupt.kvstore.model.commond.Command;
import com.cqupt.kvstore.model.sstable.SSTable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
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

    public static final String RM_MODE = "rw";

    public static final String WAL_TMP = "walTmp";

    private ConcurrentSkipListMap<String, Command> memtable;

    private ConcurrentSkipListMap<String, Command> immutableMemtable;

    private Map<Integer, List<SSTable>> levelMetaInfos = new ConcurrentHashMap<>();

    private final String dataDir;

    private final ReadWriteLock indexLock;

    private final int storeThreshold;

    private final int partSize;

    private RandomAccessFile wal;

    private File walFile;

    private final AtomicLong nextFileNumber = new AtomicLong(1);

    private Compactioner compactioner;

    public LsmKvStore(String dataDir, int storeThreshold, int partSize) {
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


            /**
             * 目录是下是空的，不用加载sstable，否则要将sstable加载进来
             */
            if(files == null || files.length == 0){
                return;
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void set(String key, String value) {

    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public void rm(String key) {

    }

    @Override
    public void close() throws IOException {

    }
}

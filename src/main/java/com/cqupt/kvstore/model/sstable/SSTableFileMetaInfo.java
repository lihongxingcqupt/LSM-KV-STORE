package com.cqupt.kvstore.model.sstable;

import lombok.Data;

/**
 * SSTable 的元信息，存储一个SSTable的基本信息
 * @Author lihongxing
 * @Date 2023/5/24 19:50
 */
@Data
public class SSTableFileMetaInfo {
    /**
     * 文件编号
     */
    private final long fileNum;

    /**
     * 文件大小
     */
    private final long fileSize;

    /**
     * 最小的key
     */
    private final String smallestKey;

    /**
     * 最大的key
     */
    private final String largestKey;

    public SSTableFileMetaInfo(long fileNum,long fileSize,String smallestKey,String largestKey){
        this.fileNum = fileNum;
        this.fileSize = fileSize;
        this.smallestKey = smallestKey;
        this.largestKey = largestKey;
    }
}

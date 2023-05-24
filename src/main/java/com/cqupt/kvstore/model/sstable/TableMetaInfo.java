package com.cqupt.kvstore.model.sstable;

import lombok.Data;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * 具体的表的元信息，其实就是磁盘上的文件的元信息，就包括了数据起始，索引起始这些了
 * @Author lihongxing
 * @Date 2023/5/24 19:59
 */
@Data
public class TableMetaInfo {
    /**
     * 版本号
     */
    private long version;

    /**
     * 文件编号
     */
    private long number;

    /**
     * 数据区开始
     */
    private long dataStart;

    /**
     * 数据区长度
     */
    private long dataLen;

    /**
     * 索引开始
     */
    private long indexStart;

    /**
     * 索引长度
     */
    private long indexLen;

    /**
     * 分段大小，用于实现稀疏索引的
     */
    private long partSize;

    /**
     * 数据文件大小
     */
    private long fileSize;

    /**
     * 最大最小key
     */
    private String smallestKey;

    private String largestKey;

    /**
     * 将元信息写入文件
     */
    public void writeToFile(RandomAccessFile file){
        try {
            // 字符串类型只能转成字节，并且长度是不一定的，因此还要将其长度也写入，读的时候才能读取出来
            file.writeBytes(smallestKey);
            file.writeInt(smallestKey.getBytes(StandardCharsets.UTF_8).length);
            file.writeBytes(largestKey);
            file.writeInt(largestKey.getBytes(StandardCharsets.UTF_8).length);

            // 后面的都是long类型，长度是确定的，因此就可以直接写入了
            file.writeLong(partSize);
            file.writeLong(dataStart);
            file.writeLong(dataLen);
            file.writeLong(indexStart);
            file.writeLong(indexLen);
            file.writeLong(version);
        }catch (Throwable t){
            throw new RuntimeException(t);
        }
    }

    /**
     * 从文件中将元信息读取出来，按照写入的顺序倒着去读取
     */
    public static TableMetaInfo readFromFile(RandomAccessFile file){
        try {
            TableMetaInfo tableMetaInfo = new TableMetaInfo();
            long fileLen = file.length();

            file.seek(fileLen - 8);
            tableMetaInfo.setVersion(file.readLong());

            file.seek(fileLen - 8 * 2);
            tableMetaInfo.setIndexLen(file.readLong());

            file.seek(fileLen - 8 * 3);
            tableMetaInfo.setIndexStart(file.readLong());

            file.seek(fileLen - 8 * 4);
            tableMetaInfo.setDataLen(file.readLong());

            file.seek(fileLen - 8 * 5);
            tableMetaInfo.setDataStart(file.readLong());

            file.seek(fileLen - 8 * 6 - 4);
            Integer largestKeyLen = file.readInt();
            file.seek(fileLen - 8 * 6 - 4 - largestKeyLen);
            byte[] largestKeyBytes = new byte[largestKeyLen];
            file.read(largestKeyBytes);
            tableMetaInfo.setLargestKey(new String(largestKeyBytes,StandardCharsets.UTF_8));

            file.seek(fileLen - 8 * 6 - 8 - largestKeyLen);
            int smallestKeyLen = file.readInt();
            file.seek(fileLen - 8 * 6 - largestKeyLen - smallestKeyLen);
            byte[] smallestKeyBytes = new byte[smallestKeyLen];
            file.read(smallestKeyBytes);
            tableMetaInfo.setSmallestKey(new String(smallestKeyBytes));

            return tableMetaInfo;
        }catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}

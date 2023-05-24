package com.cqupt.kvstore.model.sstable;

import com.alibaba.fastjson.JSONObject;
import com.cqupt.kvstore.model.Position;
import com.cqupt.kvstore.model.commond.Command;
import com.cqupt.kvstore.model.commond.RmCommand;
import com.cqupt.kvstore.model.commond.SetCommand;
import com.cqupt.kvstore.utils.FileUtils;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 维护SSTable的全部信息，另外的几个都是维护元信息的，这个是核心，可以通过方法构造SSTale，另外还可以从文件中加载SSTable
 * @Author lihongxing
 * @Date 2023/5/24 20:27
 */
@Data
public class SSTable implements Cloneable{
    private final Logger LOGGER = LoggerFactory.getLogger(SSTable.class);

    public static final String RW = "rw";

    /**
     * sstable 元数据信息
     */
    private TableMetaInfo tableMetaInfo;

    /**
     * 所处的level
     */
    private Integer level;

    /**
     * 数据库的稀疏索引
     */
    private TreeMap<String, Position> sparseIndex;

    /**
     * 文件句柄。读写文件的，按偏移量来
     */
    private final RandomAccessFile tableFile;

    /**
     * 文件路径
     */
    private final String filePath;

    /**
     * 是否支持压缩。默认采取snappy来进行压缩
     */
    private boolean enablePartDataCompress;

    /**
     * 构造函数。初始化filePath和enablePartDataCompress，另外还有别的构造函数，起到别的作用
     * @param filePath
     * @param enablePartDataCompress
     */
    private SSTable(String filePath,boolean enablePartDataCompress){
        this.filePath = filePath;
        this.enablePartDataCompress = enablePartDataCompress;
        try{
            this.tableFile = new RandomAccessFile(filePath,RW);
            tableFile.seek(0);
        }catch (Throwable t){
            throw new RuntimeException(t);
        }
    }

    /**
     * 构造函数，用文件编号、数据分区大小、是否开启压缩、位于的层数来初始化sstable
     * @param fileNumber
     * @param partSize
     * @param enablePartDataCompress
     * @param level
     */
    private SSTable(Long fileNumber,int partSize,boolean enablePartDataCompress,Integer level){
        this.tableMetaInfo = new TableMetaInfo();
        this.tableMetaInfo.setNumber(fileNumber);
        this.tableMetaInfo.setPartSize(partSize);
        this.level = level;
        this.filePath = FileUtils.buildSStableFilePath(fileNumber,level);
        this.enablePartDataCompress = enablePartDataCompress;
        try{
            this.tableFile = new RandomAccessFile(filePath,RW);
            tableFile.seek(0);
        }catch (Throwable t){
            throw new RuntimeException(t);
        }
        sparseIndex = new TreeMap<>();
    }

    /**
     * 将memtable构建成sstable
     * @param fileNumber
     * @param partSize
     * @param index
     * @param enablePartDataCompress
     * @param level
     * @return
     */
    public static SSTable createFromIndex(Long fileNumber, int partSize,
                                          ConcurrentSkipListMap<String, Command> index,
                                          boolean enablePartDataCompress,
                                          Integer level){
        SSTable ssTable = new SSTable(fileNumber,partSize,enablePartDataCompress,level);
        ssTable.initFromIndex(index);
        return ssTable;
    }

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
}

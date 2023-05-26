package com.cqupt.kvstore.utils;

import com.alibaba.fastjson.JSONObject;
import com.cqupt.kvstore.model.Position;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @Author lihongxing
 * @Date 2023/5/26 19:47
 */
public class BlockUtils {
    public final static JSONObject readJsonObject(Position position, boolean enablePartDataCompress, RandomAccessFile tableFile) throws IOException {
        // 通过 position 对象的 start 和 len 字段来读取到当前这一段数据，为什么是一段呢，因为这里的 position 对象是存在索引中的
        // 存储的格式，这一段数据开头的key作为键，position作为值，position里面封装了当前这一段数据的起始位置和长度
        byte[] dataPart = new byte[(int) position.getLen()];
        tableFile.seek(position.getStart());
        tableFile.read(dataPart);

        // 解压缩，为什么在这里解压缩而不是整体压缩呢，因为压缩的时候就是以段为单位的，解压就得以段来，否则计算就会出问题
        if(enablePartDataCompress){
            dataPart = Snappy.uncompress(dataPart);
        }
        JSONObject dataPartJson = JSONObject.parseObject(new String(dataPart));
        return dataPartJson;
    }
}

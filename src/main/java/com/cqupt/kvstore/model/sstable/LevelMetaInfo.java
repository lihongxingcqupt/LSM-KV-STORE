package com.cqupt.kvstore.model.sstable;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 存储层 sstable 信息的实体。指出当前是第几层，这一层有哪些sstable。
 * @Author lihongxing
 * @Date 2023/5/24 19:43
 */
@Data
public class LevelMetaInfo {
    /**
     * 层号
     */
    private final Integer levelNo;

    /**
     * 该层的sstable文件元数据信息
     */
    private List<SSTableFileMetaInfo> ssTableFileMetaInfos = new ArrayList<>();

    public LevelMetaInfo(Integer levelNo){
        this.levelNo = levelNo;
    }
}

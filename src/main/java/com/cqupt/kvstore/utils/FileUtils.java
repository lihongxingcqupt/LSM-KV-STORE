package com.cqupt.kvstore.utils;

import com.cqupt.kvstore.constant.KvConstant;

/**
 * 文件相关的工具类
 * @Author lihongxing
 * @Date 2023/5/24 20:54
 */

public class FileUtils {
    /**
     * 按规则来构建出sstable的全路径，也就是存储的地方
     * @param fileNumber 文件编号
     * @param level 所处的层数
     * @return
     */
    public final static String buildSStableFilePath(Long fileNumber,Integer level){
        return KvConstant.WORK_DIR + level + "_" + fileNumber + KvConstant.FILE_SUFFIX;
    }



}

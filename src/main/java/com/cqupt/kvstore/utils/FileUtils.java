package com.cqupt.kvstore.utils;

import com.cqupt.kvstore.constant.KvConstant;
import org.apache.commons.lang3.StringUtils;

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

    /**
     * 根据sstable文件全路径来解析其所处的编号
     * 例如 ： C:\Users\lihx\Desktop\game\1_11.sst 则该文件在第一层的第11个，照这样的规则解析出其编号
     */
    public final static Long parseFileNumber(String filePath){
        if(StringUtils.isBlank(filePath)){
            // 参数为空
            return -99L;
        }
        Integer lastIndex = filePath.lastIndexOf('\\');
        if(lastIndex == -1){
            return -99L;
        }

        String fileName = filePath.substring(lastIndex + 1);
        return Long.valueOf(fileName.substring(fileName.indexOf("_") + 1,fileName.indexOf('.')));
    }

    /**
     * 解析sstable所在的层
     */
    public final static Integer parseSsTableFileLevel(String filePath){
        if (StringUtils.isBlank(filePath)) {
            return -99;
        }

        Integer lastIndex = filePath.lastIndexOf('\\');
        if (lastIndex == -1) {
            return -99;
        }

        String fileName = filePath.substring(lastIndex + 1);
        return Integer.valueOf(fileName.substring(0, fileName.indexOf('_')));
    }
}

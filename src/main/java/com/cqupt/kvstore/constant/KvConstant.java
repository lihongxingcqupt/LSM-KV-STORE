package com.cqupt.kvstore.constant;

/**
 * 相关常量，抽取出来便于维护
 * @Author lihongxing
 * @Date 2023/5/23 21:14
 */
public class KvConstant {
    /**
     * 数据存储磁盘的路径
     */
    public final static String WORK_DIR = "E:\\lsm\\dbNew\\db";

    /**
     * 文件的后缀
     */
    public static final String FILE_SUFFIX = ".sst";

    /**
     * sstable 最大层数
     */

    public static final Integer SSTABLE_MAX_LEVEL = 3;
}

package com.cqupt.kvstore.service;

import java.io.Closeable;

/**
 * @Author lihongxing
 * @Date 2023/5/27 22:30
 */
public interface KvStore extends Closeable {

    /**
     * 保存数据
     */
    void set(String key,String value);

    /**
     * 获取数据，根据键
     * @Author lihongxing
     * @return
     * @param
     */
    String get(String key);

    /**
     * 删除数据
     */
    void rm(String key);


}

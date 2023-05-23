package com.cqupt.kvstore.model.commond;

/**
 * 命令接口，规范命令的行为
 * @Author lihongxing
 * @Date 2023/5/23 21:27
 */
public interface Command {
    /**
     * 每一个它的实现都要至少能够返回Key
     */
    String getKey();
}

package com.cqupt.kvstore.model.commond;

/**
 * 命令类型的枚举
 * @Author lihongxing
 * @Date 2023/5/23 21:31
 */
public enum ComandTypeEnum {
    /**
     * SET 可应用与新增和修改，修改的时候重set一条新的数据，因为查找的时候从后往前，自然就会找到新的数据，相当于是修改了
     */
    SET,

    /**
     * 删除
     */
    RM
}

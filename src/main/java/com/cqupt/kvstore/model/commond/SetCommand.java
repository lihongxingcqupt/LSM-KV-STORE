package com.cqupt.kvstore.model.commond;

import lombok.Data;

/**
 * Set 命令
 * @Author lihongxing
 * @Date 2023/5/23 21:39
 */
@Data
public class SetCommand extends AbstractCommand{
    /**
     * 数据 Key
     */
    private String Key;

    /**
     * 数据值
     */
    private String value;

    public SetCommand(String key,String value){
        super(ComandTypeEnum.SET);
        this.Key = key;
        this.value = value;
    }
}

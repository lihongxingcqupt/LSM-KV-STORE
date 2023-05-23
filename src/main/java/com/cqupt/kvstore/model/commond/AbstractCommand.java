package com.cqupt.kvstore.model.commond;

import com.alibaba.fastjson.JSON;
import lombok.Data;

/**
 * 命令的抽象类，为了程序的继承复用
 * @Author lihongxing
 * @Date 2023/5/23 21:29
 */
@Data
public abstract class AbstractCommand implements Command{
    /**
     * 命令的枚举
     */
    private ComandTypeEnum type;

    /**
     * 构造方法，为了实现它的命令的复用
     * @param type
     */
    public AbstractCommand(ComandTypeEnum type){
        this.type = type;
    }

    /**
     * 重写 toString方法
     * @return
     */
    @Override
    public String toString(){
        return JSON.toJSONString(this);
    }
}

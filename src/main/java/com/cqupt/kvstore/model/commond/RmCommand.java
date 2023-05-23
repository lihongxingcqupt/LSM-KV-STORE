package com.cqupt.kvstore.model.commond;

import lombok.Data;

/**
 * 删除命令，继承抽象类后就有了接口和抽象类的特性了
 * @Author lihongxing
 * @Date 2023/5/23 21:36
 */
@Data
public class RmCommand extends AbstractCommand{
    /**
     * 数据 Key，这里因为有 @Data 注解，因此其实重写了getKey方法了，但是好像看不出来
     */
    private String key;

    /**
     * 父类的构造方法
     * @param key
     */
    public RmCommand(String key){
        super(ComandTypeEnum.RM);
        this.key = key;
    }
}

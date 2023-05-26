package com.cqupt.kvstore.utils;

import com.alibaba.fastjson.JSONObject;
import com.cqupt.kvstore.model.commond.ComandTypeEnum;
import com.cqupt.kvstore.model.commond.Command;
import com.cqupt.kvstore.model.commond.RmCommand;
import com.cqupt.kvstore.model.commond.SetCommand;

/**
 * @Author lihongxing
 * @Date 2023/5/26 19:59
 */
public class ConvertUtil {
    public static final String TYPE = "type";

    public static Command jsonToCommand(JSONObject value){
        if(value.getString(TYPE).equals(ComandTypeEnum.SET.name())){
            return value.toJavaObject(SetCommand.class);
        }else if(value.getString(TYPE).equals(ComandTypeEnum.RM.name())){
            return value.toJavaObject(RmCommand.class);
        }
        return null;
    }
}

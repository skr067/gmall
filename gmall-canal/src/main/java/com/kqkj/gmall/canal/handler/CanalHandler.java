package com.kqkj.gmall.canal.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.base.CaseFormat;
import com.kqkj.gmall.canal.util.KafkaSender;
import com.kqkj.gmall.common.constant.GmallConstant;
import javafx.event.EventType;

import java.util.List;

public class CanalHandler {
    public static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList){
        if("people".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            //下单操作
            for (CanalEntry.RowData rowData : rowDatasList){//行集展开
                JSONObject jsonObject = new JSONObject();
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();//改制后的数据
                for (CanalEntry.Column column: afterColumnsList){//列集展开
                    System.out.println(column.getName()+":::"+column.getValue());
                    //String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    //jsonObject.put(propertyName,column.getValue());
                }
                KafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
            }
        }
    }
}

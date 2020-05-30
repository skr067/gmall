package com.kqkj.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kqkj.gmall.common.constant.GmallConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {
    private static final Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;
    @PostMapping("/log")
    public String doLog(@RequestParam("log") String logJson){
        //补时间戳
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts",System.currentTimeMillis());
        //落盘到logfile
        logger.info(jsonObject.toJSONString());
        //发送kafka
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        } else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }


        //System.out.println(logJson);
        return "success";
    }
}

package com.kqkj.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.kqkj.gmall.publisher.service.PublishService;
import com.kqkj.gmall.publisher.service.impl.PublisherServiceImpl;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublishController {

    @Autowired
    public PublisherServiceImpl publishService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date){
        List<Map> totalList = new ArrayList<>();
        Map dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Integer dauTotal = publishService.getDauTotal(date);
        totalList.add(dauMap);
        Map orderMap = new HashMap();
        orderMap.put("id","orderAmount");
        orderMap.put("name","新增交易额");
        Double orderAmout = publishService.getOrderAmout(date);
        orderMap.put("value",dauTotal);
        totalList.add(orderMap);
        return JSON.toJSONString(totalList);

    }

    @GetMapping("realtime-time")
    public String getHourTotal(@RequestParam("id") String id,@RequestParam("date") String today){
        if("dau".equals(id)) {
            Map dauHourTDMap = publishService.getDauHourMap(today);
            String yesterday = getYesterday(today);
            Map dauHourYDMap = publishService.getDauHourMap(yesterday);
            Map hourMap = new HashMap();
            hourMap.put("today", dauHourTDMap);
            hourMap.put("yesterday", dauHourYDMap);
            return JSON.toJSONString(hourMap);
        }
        return null;
    }

    private String getYesterday(String today){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = "";
        try {
            Date todayDate = simpleDateFormat.parse(today);
            Date yesterdayDate = DateUtils.addDays(todayDate, -1);
            yesterday = simpleDateFormat.format(todayDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yesterday;
    }
}

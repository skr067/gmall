package com.kqkj.gmall.publisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;


public interface PublishService {

    //查询总数
    public Integer getDauTotal(String date);
    //分时
    public Map getDauHourMap(String date);
    //金额
    public Double getOrderAmout(String date);
    //分时金额
    public Map getOrderAmoutHourMap(String date);

    public Map getSaleDetailMap(String date,String keyword,int pageNo,int pageSize,String aggsFieldName,int aggsSize);

}

package com.kqkj.gmall.mock.utils;

import java.util.Date;

public class RandomDate {

    Long logDataTime = 0L;
    int maxTimeStep = 0;

    public RandomDate(Date startDate, Date endDate, int num){
        Long avgStepTime = (endDate.getTime()-startDate.getTime())/num;
        this.maxTimeStep = avgStepTime.intValue()*2;
        this.logDataTime = startDate.getTime();
    }
}

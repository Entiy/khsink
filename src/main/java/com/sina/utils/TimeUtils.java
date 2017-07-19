package com.sina.utils;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Created by qiangshizhi on 2017/7/19.
 */
public class TimeUtils {
    static Calendar calendar = null;
    static TimeZone timeZone =null;
    static {
        timeZone =TimeZone.getTimeZone("Asia/Shanghai");
        if(timeZone == null){
            calendar = Calendar.getInstance();
        }else{
            calendar = Calendar.getInstance(timeZone);
        }
    }

    public static int getMinTh(int minutes){
        int minTh= calendar.get(Calendar.MINUTE)/minutes+calendar.get(Calendar.HOUR_OF_DAY)*(60/minutes);
        return minTh;
    }

}

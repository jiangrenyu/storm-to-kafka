package com.bonc.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
	public static void main(String[] args) {
		System.out.println(getTime(System.currentTimeMillis()));
		
	}
     //获取当前时间
	public static String getTime(long time){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(time));
	}
	
}

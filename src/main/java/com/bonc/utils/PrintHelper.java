package com.bonc.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PrintHelper {
  
  public static void print(String out){
	  System.err.println(DateUtils.getTime(new Date().getTime())+"["+Thread.currentThread().getName()+"]"+out);
  }
}

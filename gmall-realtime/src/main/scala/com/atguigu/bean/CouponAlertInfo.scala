package com.atguigu.bean

/**
 * @author zhouyanjun
 * @create 2020-11-08 22:43
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String], //ES只认java的集合，用数组保存。如果是scala集合，那就是用逗号分割的字符串了
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)


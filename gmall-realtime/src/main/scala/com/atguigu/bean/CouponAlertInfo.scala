package com.atguigu.bean

/**
 * 预警日志的样例类，对应  预警日志格式
 * @author zhouyanjun
 * @create 2020-11-08 22:43
 */
case class CouponAlertInfo(mid:String,//设备id
                           //ES只认java的集合，用数组保存。如果是scala集合，那就是用逗号分割的字符串了
                           uids:java.util.HashSet[String], //用户id
                           itemIds:java.util.HashSet[String], //优惠券涉及的商品id
                           events:java.util.List[String], // 用户行为
                           ts:Long)


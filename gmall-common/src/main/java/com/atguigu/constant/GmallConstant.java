package com.atguigu.constant;

/**
 * @author zhouyanjun
 * @create 2020-11-04 11:04
 */
public class GmallConstant {
    //启动日志主题
    public static final String KAFKA_TOPIC_STARTUP="TOPIC_STARTUP";
    //事件日志主题
    public static final String KAFKA_TOPIC_EVENT="TOPIC_EVENT";
    //订单表日志主题
    public static final String KAFKA_TOPIC_ORDER_INFO = "TOPIC_ORDER_INFO";
    //订单明细表日志主题
    public static final String KAFKA_TOPIC_ORDER_DETAIL = "TOPIC_ORDER_DETAIL";
    //用户信息表日志主题
    public static final String KAFKA_TOPIC_USER_INFO = "TOPIC_USER_INFO";
    //预警日志ES Index前缀
    public static final String ES_ALERT_INDEX_PRE = "gmall_coupon_alert";

}

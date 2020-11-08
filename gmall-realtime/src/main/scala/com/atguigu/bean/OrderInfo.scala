package com.atguigu.bean

/**
 * @author zhouyanjun
 * @create 2020-11-07 15:04
 */
case class OrderInfo(
                      id: String,
                      province_id: String,
                      consignee: String,
                      order_comment: String,
                      var consignee_tel: String, //电话
                      order_status: String,
                      payment_way: String,
                      user_id: String,
                      img_url: String,
                      total_amount: Double,
                      expire_time: String,
                      delivery_address: String,
                      create_time: String, //创建时间
                      operate_time: String,
                      tracking_no: String,
                      parent_order_id: String,
                      out_trade_no: String,
                      trade_body: String,
                      var create_date: String, //年月日
                      var create_hour: String) //小时

package com.atguigu.bean

/**
 * 事件日志样例类
 * @author zhouyanjun
 * @create 2020-11-08 22:40
 */
case class EventLog(mid:String,
                    uid:String,
                    appid:String,
                    area:String,
                    os:String,
                    `type`:String,
                    evid:String,//用户事件行为id
                    pgid:String,
                    npgid:String,
                    itemid:String,
                    var logDate:String,
                    var logHour:String,
                    var ts:Long)

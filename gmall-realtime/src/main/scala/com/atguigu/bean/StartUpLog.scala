package com.atguigu.bean

case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      `type`:String,//type是scala中的关键字
                      vs:String,
                      var logDate:String, //根据时间戳对日期和小时进行赋值，所以只能设置为var可变类型
                      var logHour:String,
                      var ts:Long)

package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zhouyanjun
 * @create 2020-11-03 21:24
 */
//@Controller
//@RestController = @Controller+@ResponseBody
@RestController //当前这个类里面所有的方法都是返回对象而不是页面
@Slf4j
public class LoggerController {
    //k-v类型
    @Autowired //加个注解，自动帮我们构建对象，我们就不用去new对象了
    //注入kafkaTemplate
    private KafkaTemplate<String, String> kafkaTemplate; //kafka生产者


    @RequestMapping("test1") //web请求当中，去访问test1这个地址

    //    @ResponseBody //告诉当前方法，不要返回页面了，要返回个对象就可以
    public String test1() {
        System.out.println("111");
        return "success";
    }


    @RequestMapping("log") //把请求与方法做一个隐射关系。隐射信息，我们写log
    //http传输中带有的参数，用logString形参来接收
    public String getLogger(@RequestParam("logString") String logString) { //接收的参数，和未来http中的参数列表一致；传入的值赋予 形参
        //1 添加时间戳
        JSONObject jsonObject = JSON.parseObject(logString); //返回jsonObject对象，对象就有方法可以操作了
        jsonObject.put("ts", System.currentTimeMillis());//就是map集合的操作，添加k-v

        //2 将jsonObject转换为字符串
        String logStr = jsonObject.toString();

        //3 将数据落盘
        log.info(logString);

        //4 根据数据类型发生至不同的主题---生产者。写的主题就是消费者要读取的主题
        if ("startup".equals(jsonObject.getString("type"))) {
            //启动日志
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,logStr);
        }else {
            //事件日志
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,logStr);
        }
        return "success";
    }
}

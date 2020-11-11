package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constant.GmallConstant;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) throws InvalidProtocolBufferException {

        //1.获取Canal连接对象，连接器。这里是我们实现idea连接canal的。用来实现我们的操作
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example", //要追踪的MySQL的实例配置  监控的哪些mysql。
                "",
                "");

        //2.抓取数据（一直在抓取）并解析
        while (true) {
            //实现真正的连接
            canalConnector.connect();
            //指定消费的数据表。类似订阅topic主题
            canalConnector.subscribe("gmall200621.*"); //整个库里的表都要
            //抓取数据Message对象。有多少拿取多少，在相应时间限制内最多100；
            Message message = canalConnector.get(100);

            //判空
            if (message.getEntries().size() <= 0) { //当前这一批次没有抓取到数据，说明mysql里没有更新数据
                System.out.println("当前没有数据！休息一会！");
                try {
                    Thread.sleep(5000);//这个方法的封装就类似kafka消费者消费数据的pull()
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //获取Entry集合
                List<CanalEntry.Entry> entries = message.getEntries();
                //遍历Entry集合
                for (CanalEntry.Entry entry : entries) { //拿到一个一个的entry
                    //获取Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //判断,只去RowData类型
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //获取表名
                        String tableName = entry.getHeader().getTableName();
                        //获取序列化的数据。序列化的数据不可以直接使用，看不懂用不了，要反序列化
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //获取数据集合。就是RowData集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //根据表名、事件类型决定把rowDatasList里面的数据发送给kafka的哪一个主题。
                        //数据类型，决定我们要哪些数据。有的表需要新增的，而有的表既要新增也要变化的
                        handler(tableName, eventType, rowDatasList);
                    }

                }

            }

        }

    }

    /**
     * 和业务相关：根据表名以及事件类型处理数据rowDatasList
     * 有下面三个参数，就可以决定把数据发送到哪个主题
     * 方法封装，使代码看着更简洁
     *
     * @param tableName    表名
     * @param eventType    事件类型
     * @param rowDatasList 数据集合
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //求GMV订单交易总额，只需要订单表的新增数据（用来累加）；而不需要变化的数据
        //对于订单表而言,只需要新增数据。INSERT就是新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sentToKafka(rowDatasList,GmallConstant.KAFKA_TOPIC_ORDER_INFO);

        }else if("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            sentToKafka(rowDatasList,GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);

        }else if("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))){
            sentToKafka(rowDatasList,GmallConstant.KAFKA_TOPIC_USER_INFO);

        }
    }

    private static void sentToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //创建、封装（转换）为JSON对象,用于存放多个列的数据
            JSONObject jsonObject = new JSONObject();
            //after是代表修改后的数据，before是修改前的数据
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue()); //封装为对象，就可以一行一行的打印，否则就是一列一列的打印
            }

            System.out.println(jsonObject.toString()); //实现一行一行的打印
            //发送数据至Kafka。发生至Topic(这里没有写死，而是变量)；发送的数据
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}

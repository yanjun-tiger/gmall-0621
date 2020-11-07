package com.atguigu.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.org.apache.xpath.internal.operations.Equals;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author zhouyanjun
 * @create 2020-11-07 8:34
 */
public class CanalClient_1duplication {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1 获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("haoop102", 11111),
                "example",
                "",
                "");
        //2 抓取数据，进行解析
        while (true) {
            //实现真正的连接
            canalConnector.connect();
            //指定消费的数据表
            canalConnector.subscribe("gmall200621.*");
            //抓取message对象
            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0) {
                System.out.println("当前没有数据，休息一下");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //获取Entry对象集合
                List<CanalEntry.Entry> entries = message.getEntries();
                //遍历Entry集合
                for (CanalEntry.Entry entry : entries) {
                    //获取Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //判断，值取ROWDATA类型 枚举类
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //获取表名
                        String tableName = entry.getHeader().getTableName();
                        //获取序列化数据
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //获取数据集合。就是RowData集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //根据表名、事件类型决定把rowDatasList里面的数据发送给kafka的哪一个主题。
                        //数据类型，决定我们要哪些数据。有的表需要新增的，而有的表既要新增也要变化的
                        handler(tableName, entryType, rowDatasList);
                    }
                }

            }
        }

    }

    private static void handler(String tableName, CanalEntry.EntryType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {


        }

    }
}

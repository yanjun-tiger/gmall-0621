package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;

/**
 * @author zhouyanjun
 * @create 2020-11-09 18:34
 */
public class EsWriterByBulk {
    public static void main(String[] args) throws IOException {
        //1 建造工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2 创建连接地址
        HttpClientConfig clientConfig = new HttpClientConfig
                .Builder("http://hadoop102:9200")
                .build();

        //设置ES连接地址
        jestClientFactory.setHttpClientConfig(clientConfig);

        //3获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //4准备数据  多条
        Movie movie1 = new Movie("1004", "金刚大战王义");
        Movie movie2 = new Movie("1005", "王义大战金刚");

        Index index1 = new Index.Builder(movie1).id("1004").build();
        Index index2 = new Index.Builder(movie2).id("1005").build();

        Bulk bulk = new Bulk.Builder() //通过.Builder来构建
                .defaultIndex("movie_test2") //默认的index，没指定index索引时，就是这个索引
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .build();

        //5写入数据 这个应该要比上面代码先写
        jestClient.execute(bulk); //放bulk对象，是action的实现类
        //6关闭客户端
        jestClient.shutdownClient();
    }
}

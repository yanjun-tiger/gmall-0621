package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author zhouyanjun
 * @create 2020-11-09 16:15
 */
public class EsWriter {
    public static void main(String[] args) throws IOException {
        //1 创建ES客户端工厂类
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2 设置连接配置参数 创建ES客户端连接地址
        HttpClientConfig clientConfig = new HttpClientConfig
                .Builder("http://hadoop102:9200")
                .build();

        //设置ES连接地址
        jestClientFactory.setHttpClientConfig(clientConfig);

        //3 获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //4 准备数据
        Movie movie = new Movie("1003","金刚川2");

        Index index = new Index.Builder(movie)
                .index("movie_test1")
                .type("_doc")
                .id("1004")
                .build();

        //5 执行插入数据操作
        jestClient.execute(index);

        //6 关闭客户端
        jestClient.shutdownClient();
    }
}

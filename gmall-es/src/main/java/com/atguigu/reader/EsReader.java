package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.MinAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author zhouyanjun
 * @create 2020-11-09 18:59
 */
public class EsReader {
    public static void main(String[] args) throws IOException {
        //1 通过工厂创建出ES客户端对象 ,胡亚征说，工厂类就类似连接池，不过连接池里都是连接，工厂类里存的都是对象
        JestClientFactory jestClientFactory =
                new JestClientFactory();

        //2 设置连接配置 创建ES客户端连接地址
        HttpClientConfig clientConfig = new HttpClientConfig //new这个HttpClientConfig对象的静态内部类对象
                .Builder("http://hadoop102:9200")  //ES服务器地址
                .build(); //build()帮助把这个对象创建出来

        //设置ES连接地址
        jestClientFactory.setHttpClientConfig(clientConfig);

        //3 获取客户端连接对象  .getObject()工厂特有获取对象的方法
        JestClient jestClient = jestClientFactory.getObject();

        //4 查询数据  如果是scala语言的话，就就是一路.下去，如果是java，那就是对象、方法、属性不断的调用
        SearchSourceBuilder searchSourceBuilder =
                new SearchSourceBuilder(); //就相当于获得整个{}包裹的查询语句

        //4.1 添加查询条件   一层方法，一层对象
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder(); //bool下面有两个平级标签，filter、must

        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("class_id","0621");//通过term精准-全值搜索匹配
        boolQueryBuilder.filter(termQueryBuilder); //这个比上一行先写

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo2","球"); //match按条件查询
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder); // 感觉这个应该先写

        //4.2 添加聚合组
        MinAggregationBuilder minAgeGroup = AggregationBuilders.min("minAge").field("age");
        searchSourceBuilder.aggregation(minAgeGroup);

        TermsAggregationBuilder byGender = AggregationBuilders.terms("countByGender").field("gender");
        searchSourceBuilder.aggregation(byGender);

        //4.3 分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);


        System.out.println(searchSourceBuilder.toString()); //以字符串形式打印查询语句
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student2")
                .addType("_doc")
                .build();

        //客户端对象执行“查询”语句，这个应该比上面先写
        SearchResult result = jestClient.execute(search);//查询的时候使用search对象

        //5 解析数据
        //5.1 获取总数
        Long total = result.getTotal();
        System.out.println("总命中" + total + "条数据");

        //5.2 获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            System.out.println("=================分割===================");
            //map数据结构进行遍历
            for (Object o : source.keySet()) {
                System.out.println("Key:" + o + "Value:" + source.get(o));
            }
        }
        //5.3 解析聚合组
        MetricAggregation aggregations = result.getAggregations();

        TermsAggregation countByGender = aggregations.getTermsAggregation("countByGender");
        for (TermsAggregation.Entry entry : countByGender.getBuckets()) {
            System.out.println(entry.getKeyAsString() + ":" + entry.getCount());
        }

        MinAggregation minAge = aggregations.getMinAggregation("minAge");
        System.out.println(minAge.getMin());
        //6关闭客户端
        jestClient.shutdownClient();
    }
}

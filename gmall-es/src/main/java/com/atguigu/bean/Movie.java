package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhouyanjun
 * @create 2020-11-09 18:30
 */

@NoArgsConstructor //无参构造器
@AllArgsConstructor // 全参构造器
@Data //get set方法
public class Movie {
    private String id;
    private String movie_name;
}

package com.atguigu.utils;

/**
 * @author zhouyanjun
 * @create 2020-11-03 20:21
 */

import java.util.Random;

public class RandomNum {
    public static int getRandInt(int fromNum, int toNum) {
        return fromNum + new Random().nextInt(toNum - fromNum + 1);
    }
}

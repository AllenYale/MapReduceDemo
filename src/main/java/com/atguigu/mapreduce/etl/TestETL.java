package com.atguigu.mapreduce.etl;

/**
 * ClassName: TestETL
 * Description:
 *
 * @ Author: Hanyuye
 * @ Date: 2023/6/26 22:45
 * @ Version: v1.0
 */
public class TestETL {
    public static void main(String[] args) {
        // 正则表达式（regular expression）
        // 校验手机号regex
        String regex = "^((13[0-9])|(14[0-9])|(15[0-9])|(17[0-9])|(18[0-9]))\\d{8}$";
        String phone = "12267076088";
        
        System.out.println(phone.matches(regex));
    }
}

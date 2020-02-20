package com.bawei.sparkproject.test;

import com.bawei.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/***
 * @author Zhi-jiang li
 * @date 2020/2/1 0001 9:22
 **/
public class JDBCHelperTest{
    public static void main(String[] args) throws Exception{
        //获取JDBCHelper单例
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        //测试普通的增删改语句
        /*jdbcHelper.executeUpdate(
                "insert into test_user(name,age) values(?,?)",
                new Object[]{"张三",28});*/

        //测试查询语句
        /*final Map<String,Object> testUser = new HashMap<String, Object>();

        jdbcHelper.executeQuery(
                "select name,age from test_user where id = ?",
                new Object[]{1},
                new JDBCHelper.QueryCallback() {
                    @Override
                    public void process(ResultSet rs) throws Exception {
                        if (rs.next()){
                            String name=rs.getString(1);
                            int age = rs.getInt(2);

                            testUser.put("name",name);
                            testUser.put("age",age);
                        }
                    }
                });

        System.out.println(testUser.get("name")+"\t"+testUser.get("age"));*/

        //测试批量代码SQL
        String sql ="insert into test_user (name,age) values (?,?)";

        List<Object[]> paramsList = new ArrayList<Object[]>();
        paramsList.add(new Object[]{"李治琦",14});
        paramsList.add(new Object[]{"小张徒弟",21});

        jdbcHelper.executeBatch(sql,paramsList);
    }
}

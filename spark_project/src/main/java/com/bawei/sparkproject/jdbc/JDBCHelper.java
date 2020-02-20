package com.bawei.sparkproject.jdbc;

import com.bawei.sparkproject.conf.ConfigurationManager;
import com.bawei.sparkproject.constant.Constants;
import jdk.nashorn.internal.scripts.JD;

import javax.xml.transform.Result;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

/***
 * 完全严格按照大公司的Coding标准来的
 * 也就是说在代码中,是不能出现Hard code(硬编码)的字符
 *
 * @author Zhi-jiang li
 * @date 2020/1/31 0031 10:54
 **/
public class JDBCHelper {

    //第一步:在静态代码块中,加载数据库的驱动
    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //第二步:实现JDBCHelper的单例化
    //内部封装一个简单的内部数据库连接池,为了保证数据库连接池有且仅有一份,所以通过单例模式

    private static JDBCHelper instance = null;

    /**
     * 私有化构造方法
     * JDBCHelper在整个程序运行周期中,只会创建一个实例
     * 在这一次创建的时候 会调用JDBCHelper()构造方法
     */

    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    private LinkedList<Connection> dataSource = new LinkedList<Connection>();

    /**
     * 第三部:实现单例的过程中,创建唯一一次的数据库连接池以及大小
     */
    private JDBCHelper() {
        //首先第一步,获取数据库连接池的大小
        int datasourceSize = ConfigurationManager.getInteger(
                Constants.JDBC_DATASOURCE_SIZE);
        //创建指定数量的数据库
        for (int i = 0; i < datasourceSize; i++) {
            String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            try {
                Connection connection = DriverManager.getConnection(url, user, password);

                dataSource.push(connection);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 第四步,提供数据库获取连接的方法
     * 设计一个简单的等待机制,防止获取的时候没有空闲的数据库连接(多线程并发)
     * synchronized 先进先出 第一个要获取的方法 先进去 其他等待 等第一个获取到 第二个进去
     */

    public synchronized Connection getConnection() {
        while (dataSource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return dataSource.poll();
    }


    /**
     * 第五步：开发增删改查的方法
     * 1、执行增删改SQL语句的方法
     * 2、执行查询SQL语句的方法
     * 3、批量执行SQL语句的方法
     */

    public int executeUpdate(String sql, Object[] params) {
        int rtn = 0;
        Connection connection = null;
        PreparedStatement pstmt = null;

        try {
            connection = getConnection();
            connection.setAutoCommit(false);

            pstmt = connection.prepareStatement(sql);

            if (params.length > 0 && params != null) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rtn = pstmt.executeUpdate();

            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                dataSource.push(connection);
            }
        }
        return rtn;
    }

    /**
     * 执行查询SQL语句
     *
     * @param sql
     * @param params
     * @param callback
     */

    public void executeQuery(String sql, Object[] params,
                             QueryCallback callback) {
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            connection = getConnection();
            pstmt = connection.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i+1,params[i]);
                }
            }

            rs = pstmt.executeQuery();

            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null){
                dataSource.push(connection);
            }
        }
    }

    /**
     * 批量执行SQL语句
     * @param sql
     * @param paramsList
     * @return 每条SQL语句影响的行数
     */

    public int[] executeBatch(String sql, List<Object[]> paramsList){
        int[] rtn = null;
        Connection connection = null;
        PreparedStatement pstmt = null;

        try {
            connection = getConnection();

            //第一步:使用Connection对象,取消自动提交
            connection.setAutoCommit(false);

            pstmt = connection.prepareStatement(sql);

            //第二步:使用pstmt.addBatch()方法加入批量sql语句
            if (paramsList.size()>0 && paramsList != null){
                for (Object[] params : paramsList) {
                    for(int i = 0 ; i < params.length ; i++){
                        pstmt.setObject(i+1,params[i]);
                    }
                    pstmt.addBatch();
                }
            }

            //第三部使用executeBatch方法,执行所有sql语句
            pstmt.executeBatch();

            //最后一步使用:Connection对象,提交批量sql语句
            connection.commit();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (connection != null){
                dataSource.push(connection);
            }
        }
        return rtn;
    }

    /**
     * 静态内部类：查询回调接口
     *
     * @author Administrator
     */
    public static interface QueryCallback {
        /**
         * 处理查询结果
         *
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }

}

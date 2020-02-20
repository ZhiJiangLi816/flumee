package com.bawei.sparkproject.dao.impl;

import com.bawei.sparkproject.dao.ITop10CategoryDAO;
import com.bawei.sparkproject.domain.Top10Category;
import com.bawei.sparkproject.jdbc.JDBCHelper;

/***
 * @author Zhi-jiang li
 * @date 2020/2/6 0006 17:05
 **/
public class ITop10CategoryDAOImpl implements ITop10CategoryDAO {
    @Override
    public void insert(Top10Category top10Category) {
        String sql = "insert into top10_category values (?,?,?,?,?)";
        Object[] params = new Object[]{
                top10Category.getTaskid(),
                top10Category.getCategoryid(),
                top10Category.getClickCount(),
                top10Category.getOrderCount(),
                top10Category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}

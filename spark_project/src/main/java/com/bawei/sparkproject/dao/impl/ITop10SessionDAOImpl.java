package com.bawei.sparkproject.dao.impl;

import com.bawei.sparkproject.dao.ITop10SessionDAO;
import com.bawei.sparkproject.domain.Top10Session;
import com.bawei.sparkproject.jdbc.JDBCHelper;

/***
 * @author Zhi-jiang li
 * @date 2020/2/7 0007 15:57
 **/
public class ITop10SessionDAOImpl implements ITop10SessionDAO {
    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_session values(?,?,?,?)";
        Object[] paramas = new Object[]{
                top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,paramas);

    }
}

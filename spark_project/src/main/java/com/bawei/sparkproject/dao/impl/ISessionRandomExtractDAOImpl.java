package com.bawei.sparkproject.dao.impl;

import com.bawei.sparkproject.dao.ISessionRandomExtract;
import com.bawei.sparkproject.domain.SessionRandomExtract;
import com.bawei.sparkproject.jdbc.JDBCHelper;

/***
 * 随机抽取session的DAO实现
 * @author Zhi-jiang li
 * @date 2020/2/5 0005 15:57
 **/
public class ISessionRandomExtractDAOImpl implements ISessionRandomExtract {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";
        Object[] paramas = new Object[]{
                sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,paramas);
    }
}

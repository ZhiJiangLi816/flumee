package com.bawei.sparkproject.dao;

import com.bawei.sparkproject.domain.SessionAggrStat;

/***
 * session聚合统计模块DAO接口
 * @author Zhi-jiang li
 * @date 2020/2/4 0004 17:05
 **/
public interface ISessionAggrStatDAO {

    /**
     * 插入session聚合统计结果
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);
}

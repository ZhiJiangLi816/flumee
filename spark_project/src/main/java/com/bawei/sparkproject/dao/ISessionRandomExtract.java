package com.bawei.sparkproject.dao;

import com.bawei.sparkproject.domain.SessionAggrStat;
import com.bawei.sparkproject.domain.SessionRandomExtract;

/***
 * session随机抽取模块DAO接口
 * @author Zhi-jiang li
 * @date 2020/2/4 0004 17:05
 **/
public interface ISessionRandomExtract {

    /**
     * 插入session随机抽取
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);
}

package com.bawei.sparkproject.dao;

import com.bawei.sparkproject.domain.SessionAggrStat;
import com.bawei.sparkproject.domain.SessionDetail;

import java.util.List;

/***
 * session明细DAO接口
 * @author Zhi-jiang li
 * @date 2020/2/4 0004 17:05
 **/
public interface ISessionDetailDAO {

    /**
     * 插入一条session明细
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);


    /**
     * 插入多条session明细
     * @param sessionDetails
     */
    void insertBatch(List<SessionDetail> sessionDetails);
}

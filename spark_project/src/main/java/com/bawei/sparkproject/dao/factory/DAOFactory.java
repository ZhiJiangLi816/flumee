package com.bawei.sparkproject.dao.factory;

import com.bawei.sparkproject.dao.*;
import com.bawei.sparkproject.dao.impl.*;

/***
 * @author Zhi-jiang li
 * @date 2020/2/2 0002 9:42
 **/
public class DAOFactory {
    /**
     * 获取任务管理DAO
     * @return
     */
    public static ITaskDAO getTaskDAO(){
        return new ITaskDaoImpl();
    }

    /**
     * 获取session聚合统计DAO
     * @return
     */
    public static ISessionAggrStatDAO getSessionAggrStatDAO(){
        return new ISessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtract getSessionRandomExtract(){
        return new ISessionRandomExtractDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAL(){
        return new ISessionDetailDAOImpl();
    }

    public static ITop10CategoryDAO getTop10CategoryDAO(){
        return new ITop10CategoryDAOImpl();
    }

    public static ITop10SessionDAO getTop10SessionDAO(){
        return new ITop10SessionDAOImpl();
    }
}

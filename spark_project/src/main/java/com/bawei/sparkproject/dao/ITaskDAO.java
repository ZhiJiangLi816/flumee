package com.bawei.sparkproject.dao;

import com.bawei.sparkproject.domain.Task;

/***
 * @author Zhi-jiang li
 * @date 2020/2/2 0002 9:34
 **/
public interface ITaskDAO {
    /**
     * 通过ID查找task中的一条数据
     * @param taskId
     * @return
     */
    Task findById(long taskId);

}

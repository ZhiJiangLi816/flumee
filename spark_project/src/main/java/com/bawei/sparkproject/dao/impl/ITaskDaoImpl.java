package com.bawei.sparkproject.dao.impl;

import com.bawei.sparkproject.dao.ITaskDAO;
import com.bawei.sparkproject.domain.Task;
import com.bawei.sparkproject.jdbc.JDBCHelper;

import java.sql.Connection;
import java.sql.ResultSet;

/***
 * @author Zhi-jiang li
 * @date 2020/2/2 0002 9:37
 **/
public class ITaskDaoImpl implements ITaskDAO {
    @Override
    public Task findById(long taskId) {
        final Task task = new Task();

        String sql = "select * from task where task_id = ?";
        Object[] params = new Object[]{taskId};

        final JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()){
                    long taskid = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskid(taskid);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });

        return task;
    }
}

package com.bawei.sparkproject.test;

import com.bawei.sparkproject.dao.ITaskDAO;
import com.bawei.sparkproject.dao.factory.DAOFactory;
import com.bawei.sparkproject.dao.impl.ITaskDaoImpl;
import com.bawei.sparkproject.domain.Task;

/***
 * @author Zhi-jiang li
 * @date 2020/2/2 0002 9:48
 **/
public class TaskDAOTest {
    public static void main(String[] args) {
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1);
        System.out.println(task.getTaskName());
    }
}

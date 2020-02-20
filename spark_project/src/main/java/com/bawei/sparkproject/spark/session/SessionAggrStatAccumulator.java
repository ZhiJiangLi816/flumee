package com.bawei.sparkproject.spark.session;

import com.bawei.sparkproject.constant.Constants;
import com.bawei.sparkproject.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/***
 * @author Zhi-jiang li
 * @date 2020/2/4 0004 10:55
 **/
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    /**
     * zero方法,主要用于数据的初始化
     * 返回一个值,就是初始化中,所有范围区间内的数量,都是0
     *
     * @param v
     * @return
     */
    @Override
    public String zero(String v) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * addInPlace,addAccumulator可以理解为一样的
     * 这两个方法,主要是实现,v1是我们初始化的连接串
     * v2就是我们在遍历Session的时候,判断出某个session对应的区间,
     * 然后用Constants.TIME_PERIOD_1s_3s
     * 在v1中,找到v2对应的value,累加1,然后再重新回连接串里面去
     *
     * @param v1
     * @param v2
     * @return
     */
    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1,v2);
    }

    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1,v2);
    }

    /**
     * session统计计算逻辑
     * @param v1 连接串
     * @param v2 范围区间
     * @return 更新以后的连接串
     */
    private String add(String v1, String v2) {

        //判断v1为空,返回v2
        if (StringUtils.isEmpty(v1)){
            return v2;
        }

        //使用工具类,从v1中提取v2对应的值,并累加1
        String oldValue =StringUtils.getFieldFromConcatString(v1,"\\|",v2);
        if (oldValue != null) {
            //将范围内区间原有的值,累加1
            int newValue = Integer.valueOf(oldValue)+1;

            return StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue));
        }

        return v1;
    }
}

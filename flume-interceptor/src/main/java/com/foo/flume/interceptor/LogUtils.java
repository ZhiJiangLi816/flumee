package com.foo.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/***
 * @author Zhi-jiang li
 * @date 2019/12/31 0031 15:35
 **/
public class LogUtils {
    private static Logger logger = LoggerFactory.getLogger(LogUtils.class);

    public static boolean validateReportLog(String log) {
        //1577435668032|{"cm":{"ln":"-93.5","sv":"V2.2.5","os":"8.2.4","g":"3Q4232G7@gmail.com","mid":"m834","nw":"WIFI","l":"en","vc":"4","hw":"640*960","ar":"MX","uid":"u393","t":"1577377494847","la":"27.0","md":"Huawei-16","vn":"1.1.4","ba":"Huawei","sr":"C"},"ap":"gmall","et":[{"ett":"1577380593757","en":"loading","kv":{"extend2":"","loading_time":"0","action":"2","extend1":"","type":"2","type1":"542","loading_way":"1"}},{"ett":"1577396367231","en":"ad","kv":{"entry":"2","show_style":"1","action":"5","detail":"201","source":"4","behavior":"1","content":"1","newstype":"9"}},{"ett":"1577339228851","en":"active_background","kv":{"active_source":"3"}},{"ett":"1577418372692","en":"favorites","kv":{"course_id":6,"id":0,"add_time":"1577424497363","userid":1}},{"ett":"1577339443030","en":"praise","kv":{"target_id":7,"id":3,"type":1,"add_time":"1577336877570","userid":0}}]}
        try {
            //首先校验的是总长度(2)
            if (log.split("\\|").length<2){
                return false;
            }

            //其次校验的是第一串是否为时间戳
            if (log.split("\\|")[0].length()!=13 || !NumberUtils.isDigits(log.split("\\|")[0])){
                return false;
            }

            //再次判断第二个|之后的是否为正确json串
            if (!log.split("\\|")[1].trim().startsWith("{") || !log.split("\\|")[1].trim().endsWith("}")){
                return  false;
            }
        }catch (Exception e){
            //错误打印的日志
            logger.error("error parse,message is:" + log);
            logger.error(e.getMessage());
            return false;
        }

        return true;
    }
}

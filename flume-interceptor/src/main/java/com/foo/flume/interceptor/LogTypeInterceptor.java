package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/***
 * @author Zhi-jiang li
 * @date 2020/1/1 0001 15:04
 **/
public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取flume接收消息头
        Map<String, String> headers = event.getHeaders();
        //获取flume接受的json数据数组
        byte[] json = event.getBody();

        //将json数组转换为字符串
        String jsonStr = new String(json);

        String logType = "";

        //开始判断日志类型
        if (jsonStr.contains("start")){
            logType = "start";
        }else{
            logType = "event";
        }

        //将日志类型存储到flume头部中
        headers.put("logType",logType);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> interceptors = new ArrayList<>(events.size());

        for (Event event : events) {
            Event interceptEvent = intercept(event);
            if (interceptEvent!=null){
                interceptors.add(interceptEvent);
            }
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

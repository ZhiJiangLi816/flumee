package com.foo.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/***
 * @author Zhi-jiang li
 * @date 2019/12/31 0031 15:32
 **/
public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取传输过来的数据
        String body = new String(event.getBody(), Charset.forName("UTF-8"));

        //调用工具类,此处body为原始数据,需要处理
        if (LogUtils.validateReportLog(body)){
            //通过了校验就是我们要的目标数据
            return event;
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> eventList = new ArrayList<>(events.size());

        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            //如果不为空则添加到集合里面
            if (interceptedEvent != null){
                eventList.add(interceptedEvent);
            }
        }
        return eventList;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

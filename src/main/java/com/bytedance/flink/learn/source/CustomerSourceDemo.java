package com.bytedance.flink.learn.source;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;

public class CustomerSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new MySourceFunction())
                .setParallelism(2)
                .map(JSON::toJSONString)
                .print();

        env.execute();
    }
}

class MySourceFunction extends RichParallelSourceFunction<EventLog> {
    volatile boolean flag = true;


    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {
        EventLog eventLog = new EventLog();
        String[] events = new String[]{"appLaunch", "pageLoad", "adShow", "adClick", "itemShare", "putBack", "appClose"};
        HashMap<String, String> eventInfo = new HashMap<>();
        while (flag) {
            eventLog.setGuid(RandomUtils.nextLong(1, 10000));
            eventLog.setEventId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimestamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0, events.length)]);
            eventInfo.put(RandomStringUtils.randomAlphabetic(2), RandomStringUtils.randomAlphabetic(4));
            eventLog.setEventInfo(eventInfo);
            ctx.collect(eventLog);
            eventInfo.clear();
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}


@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
class EventLog {
    private long guid;
    private String sessionId;
    private String eventId;
    private long timestamp;
    private Map<String, String> eventInfo;
}

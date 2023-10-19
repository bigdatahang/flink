package com.bytedance.flink.learn.source;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class CustomerSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<EventLog> stream = env.addSource(new MySourceFunction());
        SinkFunction<EventLog> jdbcSink = JdbcSink.sink(
                "insert into t_event values(?, ?, ?, ?, ?) on duplicate key update sessionId = ?, eventId = ?, ts = ?, eventInfo = ?",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventLog eventLog) throws SQLException {
                        preparedStatement.setLong(1, eventLog.getGuid());
                        preparedStatement.setString(2, eventLog.getSessionId());
                        preparedStatement.setString(3, eventLog.getEventId());
                        preparedStatement.setLong(4, eventLog.getTimestamp());
                        preparedStatement.setString(5, JSON.toJSONString(eventLog.getEventInfo()));
                    }
                }, JdbcExecutionOptions
                        .builder()
                        .withBatchSize(5)
                        .withMaxRetries(2)
                        .build(),
                JdbcConnectorOptions
                        .builder()
                        .setDBUrl("jdbc:mysql://hadoop102:3306")
                        .setDriverName("com.mysql.cj.Driver")
                        .setUsername("root")
                        .setPassword("000000")
                        .build()
        );
        stream.addSink(jdbcSink);
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

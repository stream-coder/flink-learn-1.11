package com.github.sc.flink.stream.aggregate;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author GangW
 */
public class SampleAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromCollection(types()).keyBy("name").window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .sum("count").print();

        env.execute();
    }

    public static List<AggType> types() throws ParseException {
        List<AggType> list = new ArrayList<>();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = sdf.parse("2020-08-31 01:01:01").getTime();

        for (int i = 0; i < 100; i++) {
            AggType type = new AggType();
            type.setName("name");
            type.setCount(1);
            type.setTime(start + (i * 1000));

            list.add(type);
        }

        return list;
    }

    public static class AggType {
        public AggType() {}

        public AggType(String name, long count, long time) {
            this.name = name;
            this.count = count;
            this.time = time;
        }

        private String name;
        private long count;
        private long time;

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }
}

package com.github.sc.flink.stream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author GangW
 */
public class SampleCode {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("abc").print();

        env.execute("sample");
    }
}

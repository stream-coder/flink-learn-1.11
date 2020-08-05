package com.github.sc.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author GangW
 */
public class KafkaSourceSql {
    public static final String KAFKA_TABLE_SOURCE_DDL =
    // @formatter:off
        "CREATE TABLE user_behavior ("
            + " user_id BIGINT,"
            + " item_id BIGINT,"
            + " category_id BIGINT,"
            + " behavior STRING,"
            + " ts TIMESTAMP(3)"
            + ") WITH ("
            + " 'connector.type' = 'kafka',  -- 指定连接类型是kafka"
            + " 'connector.version' = 'universal',  -- 与我们之前Docker安装的kafka版本要一致"
            + " 'connector.topic' = 'flink_sql_case1', -- 之前创建的topic"
            + " 'connector.properties.group.id' = 'flink-test-0', -- 消费者组，相关概念可自行百度"
            + " 'connector.startup-mode' = 'earliest-offset',  --指定从最早消费"
            + " 'connector.properties.bootstrap.servers' = 'localhost:9092',  -- broker地址"
            + " 'format.type' = 'json'  -- json格式，和topic中的消息格式保持一致"
            + ")";
    //@formatter: on
    
    public static void main(String[] args) throws Exception {
        // 构建StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构建EnvironmentSettings 并指定Blink Planner
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        // 构建StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        // 通过DDL，注册kafka数据源表
        tEnv.sqlUpdate(KAFKA_TABLE_SOURCE_DDL);

        // 执行查询
        Table table = tEnv.sqlQuery("select item_id from user_behavior");

        // 转回DataStream并输出
        tEnv.toAppendStream(table, Row.class).print().setParallelism(1);

        // 任务启动，这行必不可少！
        env.execute("test");
    }
}

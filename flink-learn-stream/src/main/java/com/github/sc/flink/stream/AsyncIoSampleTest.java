package com.github.sc.flink.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.AsyncClient;

/**
 * @author GangW
 */
public class AsyncIoSampleTest {
    private int count = 100000;

    @Test
    public void syncIo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromCollection(keys()).map(new RichMapFunction<String, String>() {
            private AerospikeClient client;

            @Override
            public void open(Configuration parameters) throws Exception {
                client = new AerospikeClient("localhost", 3000);
            }

            @Override
            public void close() throws Exception {
                client.close();
            }

            @Override
            public String map(String s) throws Exception {
                client.get(null, new Key("bsfit", "frms", s));
                return s;
            }
        }).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {}
        });

        long start = System.currentTimeMillis();
        env.execute("test");
        long end = System.currentTimeMillis();

        System.out.println(end - start);

        // 109183
    }

    @Test
    public void asyncIo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> source = env.fromCollection(keys());

        AsyncDataStream.unorderedWait(source, new RichAsyncFunction<String, String>() {
            private ExecutorService service;
            private AsyncClient client;

            @Override
            public void open(Configuration parameters) throws Exception {
                service = Executors.newFixedThreadPool(10);
                client = new AsyncClient("localhost", 3000);
            }

            @Override
            public void close() throws Exception {
                service.shutdown();
                while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
                    System.out.println("await termination");
                }
                client.close();
            }

            @Override
            public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
                service.submit(() -> {
                    client.get(null, new Key("bsfit", "frms", input));

                    resultFuture.complete(Collections.singleton(input));
                });
            }
        }, 1000, TimeUnit.MILLISECONDS, 500).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {}
        });

        long start = System.currentTimeMillis();
        env.execute("test");
        long end = System.currentTimeMillis();

        System.out.println(end - start);

        // 22081
    }

    @Test
    public void multiThread() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> source = env.fromCollection(keys());

        source.timeWindowAll(Time.seconds(1)).process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
            private ExecutorService service;
            private AsyncClient client;
            private int threadCount = 10;

            @Override
            public void open(Configuration parameters) throws Exception {
                service = Executors.newFixedThreadPool(threadCount);
                client = new AsyncClient("localhost", 3000);
            }

            @Override
            public void close() throws Exception {
                service.shutdown();
                while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
                    System.out.println("await termination");
                }
                client.close();
            }

            @Override
            public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                List<String> ids = new ArrayList<>();

                List<Callable<String>> callables = new ArrayList<>();
                for (String s : elements) {
                    callables.add(() -> {
                        client.get(null, new Key("bsfit", "frms", s));
                        return s;
                    });
                }

                List<Future<String>> futures = service.invokeAll(callables);

                for (Future<String> f : futures) {
                    String s = f.get();
                    System.out.println(s);
                    out.collect(s);
                }
            }
        }).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        long start = System.currentTimeMillis();
        env.execute("test");
        long end = System.currentTimeMillis();

        System.out.println(end - start);
    }

    private List<String> keys() {
        List<String> str = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            str.add(UUID.randomUUID().toString());
        }

        return str;
    }
}

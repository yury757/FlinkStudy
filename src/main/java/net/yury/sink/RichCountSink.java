package net.yury.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.atomic.AtomicInteger;

public class RichCountSink extends RichSinkFunction<String> {
    private static final AtomicInteger globalOpenCount = new AtomicInteger(0);
    private static final AtomicInteger globalCloseCount = new AtomicInteger(0);
    private static final AtomicInteger instanceCount = new AtomicInteger(0);

    @Override
    public void open(Configuration parameters) throws Exception {
        int count = globalOpenCount.getAndIncrement();
        System.out.println("第" + count + "次open! hash code: " + this.hashCode());
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        int count = instanceCount.getAndIncrement();
        System.out.println("instanceCount: " + count + ", value: " + value);
    }

    @Override
    public void close() {
        int count = globalCloseCount.getAndIncrement();
        System.out.println("第" + count + "次close! hash code: " + this.hashCode());
    }
}

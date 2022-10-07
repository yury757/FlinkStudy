package net.yury.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class PrintSink<IN> implements SinkFunction<IN> {
    @Override
    public void invoke(IN value, Context context) throws Exception {
        System.out.println(value.toString());
    }

}

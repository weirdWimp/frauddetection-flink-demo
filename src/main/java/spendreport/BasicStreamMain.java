package spendreport;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * @author guo
 * @date 2021/7/29
 */
public class BasicStreamMain {

    public static class MaxByFunction extends RichFlatMapFunction<Long, Long> {

        /**
         * keyed state for each key (here is accountId)
         */
        private transient ValueState<Long> maxState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("maxFlag", Types.LONG);
            maxState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void flatMap(Long value, Collector<Long> out) throws Exception {
            if (maxState.value() == null || value > maxState.value()) {
                out.collect(value);
                maxState.update(value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Long> unboundedNums = LongStream.rangeClosed(1, 100)
                .map(i -> 100 - i + 1)
                .boxed()
                .collect(Collectors.toList());

        DataStream<Long> eventNumbers = env.fromCollection(unboundedNums)
                .keyBy(i -> i % 2)
                .flatMap(new MaxByFunction());

        eventNumbers.print();
        env.execute();
    }
}

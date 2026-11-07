package com.example.flink.functions;

import com.example.flink.dto.TickOutRecord;
import com.example.flink.dto.TickRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MovingAvgCalculator extends KeyedProcessFunction<String, TickRecord, TickOutRecord> {

    private final int period;
    private transient ListState<Double> priceState;
    private final transient DateTimeFormatter formatter;

    public MovingAvgCalculator(int period) {
        this.period = period;
        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withZone(ZoneId.systemDefault());
    }


    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        ListStateDescriptor<Double> descriptor = new ListStateDescriptor<>("price-history", Double.class);
        priceState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(TickRecord value, KeyedProcessFunction<String, TickRecord, TickOutRecord>.Context ctx, Collector<TickOutRecord> out) throws Exception {
        List<Double> prices = new ArrayList<>();
        double sum = 0.0;

        for (Double p : priceState.get()) {
            prices.add(p);
            sum += p;
        }

        prices.add(value.close());

        sum += value.close();

        if (prices.size() > period) {
            double removed = prices.remove(0);
            sum -= removed;
        }

        priceState.update(prices);

        double avg = sum / prices.size();

        out.collect(new TickOutRecord(
                avg,
                0,
                value.timestamp()
            )
        );
    }
}

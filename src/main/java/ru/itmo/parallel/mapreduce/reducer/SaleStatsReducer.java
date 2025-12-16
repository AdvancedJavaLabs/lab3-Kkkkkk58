package ru.itmo.parallel.mapreduce.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.itmo.parallel.mapreduce.writable.SalesWritable;

public class SaleStatsReducer extends Reducer<Text, SalesWritable, Text, SalesWritable> {

    private final SalesWritable result = new SalesWritable();

    @Override
    protected void reduce(Text key, Iterable<SalesWritable> values, Context context) throws IOException, InterruptedException {
        double totalRevenue = 0.0;
        long totalQuantity = 0;

        for (SalesWritable value : values) {
            totalRevenue += value.getRevenue();
            totalQuantity += value.getQuantity();
        }

        result.setRevenue(totalRevenue);
        result.setQuantity(totalQuantity);
        context.write(key, result);
    }
}


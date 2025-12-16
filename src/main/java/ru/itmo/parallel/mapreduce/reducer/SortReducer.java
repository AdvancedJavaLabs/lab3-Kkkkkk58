package ru.itmo.parallel.mapreduce.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.itmo.parallel.mapreduce.writable.RevenueKeyWritable;

import static ru.itmo.parallel.mapreduce.common.MapReduceConstants.DEFAULT_SEPARATOR;

public class SortReducer extends Reducer<RevenueKeyWritable, Text, NullWritable, Text> {

    private final Text result = new Text();

    @Override
    protected void reduce(RevenueKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split(DEFAULT_SEPARATOR);
            result.set(parts[0] + DEFAULT_SEPARATOR + String.format("%.2f", key.getRevenue()) + DEFAULT_SEPARATOR + parts[1]);
            context.write(NullWritable.get(), result);
        }
    }
}


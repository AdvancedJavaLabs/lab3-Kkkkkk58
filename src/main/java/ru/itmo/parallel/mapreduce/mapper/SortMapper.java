package ru.itmo.parallel.mapreduce.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.itmo.parallel.mapreduce.writable.RevenueKeyWritable;

import static ru.itmo.parallel.mapreduce.common.MapReduceConstants.DEFAULT_SEPARATOR;

public class SortMapper extends Mapper<LongWritable, Text, RevenueKeyWritable, Text> {

    private final RevenueKeyWritable revenueKey = new RevenueKeyWritable();
    private final Text value = new Text();

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String[] parts = line.toString().split(DEFAULT_SEPARATOR);
        if (parts.length >= 3) {
            String category = parts[0];
            double revenue = Double.parseDouble(parts[1]);
            String quantity = parts[2];

            revenueKey.setRevenue(revenue);
            value.set(category + DEFAULT_SEPARATOR + quantity);
            context.write(revenueKey, value);
        }
    }
}


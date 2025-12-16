package ru.itmo.parallel.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itmo.parallel.mapreduce.job.SaleStatsJob;
import ru.itmo.parallel.mapreduce.job.SortJob;

public class SalesApp extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(SalesApp.class);

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new SalesApp(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        java.util.List<String> filtered = new java.util.ArrayList<>();
        for (String arg : args) {
            if (!arg.contains(".") && !arg.startsWith("-D")) {
                filtered.add(arg);
            }
        }
        String[] cleanArgs = filtered.toArray(new String[0]);

        if (cleanArgs.length < 2) {
            throw new IllegalArgumentException("Pass parameters <input> <output> [numReducers]");
        }

        String input = cleanArgs[0];
        String output = cleanArgs[1];
        String intermediate = output + "_intermediate";
        int numReducers = cleanArgs.length > 2 ? Integer.parseInt(cleanArgs[2]) : 1;

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(intermediate), true);
        fs.delete(new Path(output), true);

        long start = System.currentTimeMillis();

        LOG.info("Starting sales aggregation");
        int aggregationResult = ToolRunner.run(conf, new SaleStatsJob(), new String[]{input, intermediate, String.valueOf(numReducers)});
        if (aggregationResult != 0) {
            return aggregationResult;
        }
        LOG.info("Aggregation is done after {}ms", System.currentTimeMillis() - start);

        LOG.info("Starting sorting");
        long timeBeforeSorting = System.currentTimeMillis();
        int sortingResult = ToolRunner.run(conf, new SortJob(), new String[]{intermediate, output});
        if (sortingResult != 0) {
            return sortingResult;
        }
        LOG.info("Sorting is done after {}ms", System.currentTimeMillis() - timeBeforeSorting);

        fs.delete(new Path(intermediate), true);
        LOG.info("All data is processed after {}ms", System.currentTimeMillis() - start);
        return 0;
    }
}

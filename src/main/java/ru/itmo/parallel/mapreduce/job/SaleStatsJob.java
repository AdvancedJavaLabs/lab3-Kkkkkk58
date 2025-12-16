package ru.itmo.parallel.mapreduce.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import ru.itmo.parallel.mapreduce.mapper.SaleStatsMapper;
import ru.itmo.parallel.mapreduce.reducer.SaleStatsReducer;
import ru.itmo.parallel.mapreduce.writable.SalesWritable;

public class SaleStatsJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        int numReducers = args.length > 2 ? Integer.parseInt(args[2]) : 1;

        Job job = Job.getInstance(getConf(), "Sale Stats");
        job.setJarByClass(SaleStatsJob.class);

        job.setMapperClass(SaleStatsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesWritable.class);

        // job.setCombinerClass(SaleStatsReducer.class);
        job.setReducerClass(SaleStatsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SalesWritable.class);
        job.setNumReduceTasks(numReducers);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

package ru.itmo.parallel.mapreduce.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import ru.itmo.parallel.mapreduce.mapper.SortMapper;
import ru.itmo.parallel.mapreduce.reducer.SortReducer;
import ru.itmo.parallel.mapreduce.writable.RevenueKeyWritable;

public class SortJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(getConf(), "Sales Sort");
        job.setJarByClass(SortJob.class);

        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(RevenueKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

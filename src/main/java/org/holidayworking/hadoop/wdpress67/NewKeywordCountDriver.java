package org.holidayworking.hadoop.wdpress67;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NewKeywordCountDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.printf("Usage: %s [generic options] <indir> <intermediate outdir> <outdir>\n", getClass().getSimpleName());
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path intermediatePath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Job job = new Job(getConf(), "KeywordCount");
        job.setJarByClass(NewKeywordCountDriver.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, intermediatePath);
        job.setMapperClass(KeywordMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        boolean let = job.waitForCompletion(true);
        if (!let) {
            return -1;
        }

        Job secondJob = new Job(getConf(), "Sort");
        secondJob.setJarByClass(NewKeywordCountDriver.class);
        FileInputFormat.addInputPath(secondJob, intermediatePath);
        FileOutputFormat.setOutputPath(secondJob, outputPath);
        secondJob.setMapperClass(CountMapper.class);
        secondJob.setReducerClass(OutputReducer.class);
        secondJob.setSortComparatorClass(InverseComparator.class);
        secondJob.setPartitionerClass(CountPartitioner.class);
        secondJob.setMapOutputKeyClass(IntWritable.class);
        secondJob.setMapOutputValueClass(Text.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(IntWritable.class);
        return secondJob.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new NewKeywordCountDriver(), args));
    }

}

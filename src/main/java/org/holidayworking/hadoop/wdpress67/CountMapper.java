package org.holidayworking.hadoop.wdpress67;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] records = line.split("\t");
        if (records.length == 2) {
            context.write(new IntWritable(Integer.parseInt(records[1])), new Text(records[0]));
        }
    }

}

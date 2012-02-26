package org.holidayworking.hadoop.wdpress67;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CountPartitioner extends Partitioner<IntWritable, Text> {

    @Override
    public int getPartition(IntWritable key, Text value, int numPartitions) {
        if (numPartitions > 1 && key.get() > 20) {
            return 1;
        }
        return 0;
    }

}

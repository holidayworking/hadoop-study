package org.holidayworking.hadoop.japanese;

import java.io.IOException;

import net.reduls.gomoku.Morpheme;
import net.reduls.gomoku.Tagger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        for (Morpheme m : Tagger.parse(line)) {
            if (m.feature.startsWith("名詞")) {
                context.write(new Text(m.surface), new IntWritable(1));
            }
        }
    }

}
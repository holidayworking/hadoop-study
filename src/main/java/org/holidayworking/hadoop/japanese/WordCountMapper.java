package org.holidayworking.hadoop.japanese;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.atilika.kuromoji.Token;
import org.atilika.kuromoji.Tokenizer;

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Tokenizer tokenizer = Tokenizer.builder().build();
        String line = value.toString();
        List<Token> result = tokenizer.tokenize(line);
        for (Token token : result) {
            context.write(new Text(token.getSurfaceForm()), new IntWritable(1));
        }
    }

}
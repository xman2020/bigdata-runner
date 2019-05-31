/**
 * xxx
 * Copyright (c) 2012-2018 All Rights Reserved.
 */
package x.bigdata.test.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 单词统计Reducer
 * 
 * @author xman Sep 6, 2018
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        int count = 0;

        for (IntWritable value : values) {
            count += value.get();
        }

        context.write(key, new IntWritable(count));
    }
}

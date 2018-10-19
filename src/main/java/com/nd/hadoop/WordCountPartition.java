package com.nd.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author: oubin
 * @date: 2018/10/17 15:30
 * @Description:
 */
public class WordCountPartition extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {

        return text.toString().hashCode()%i;
    }
}

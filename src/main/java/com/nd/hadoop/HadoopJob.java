package com.nd.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: oubin
 * @date: 2018/9/26 17:03
 * @Description:
 */
public class HadoopJob {
    public static void main(String[] args) throws IOException {

        Configuration config = new Configuration();
        //设置hdfs的通讯地址
        config.set("fs.defaultFS", "hdfs://node1:9000");
        config.set("mapreduce.framework.name", "yarn");
        config.set("yarn.resourcemanager.hostname", "node1");

        try {
            FileSystem fs = FileSystem.get(config);

            Job job = Job.getInstance(config);
            job.setJarByClass(HadoopJob.class);

            job.setJobName("ABCD");

            /**
             * 设置Map
             */
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            /**
             * 加入自定义分区定义
             */
            job.setPartitionerClass(WordCountPartition.class);
            job.setNumReduceTasks(2);

            /**
             * 加入排序定义
             */
            job.setSortComparatorClass(WordCountSort.class);

            /**
             * 设置Reduce
             */
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));

            Path outpath = new Path(args[1]);
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);

            boolean success = job.waitForCompletion(true);
            if (success) {
                System.out.println("job任务执行成功");
                System.exit(0);
            }else {
                System.out.println("job任务执行失败");
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

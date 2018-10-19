package com.nd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * @author: oubin
 * @date: 2018/9/30 10:26
 * @Description:
 */
public class TopNSpark implements Serializable {

    private JavaSparkContext jsc;
    private Broadcast<Integer> topNum;
    private String inputPath;

    public TopNSpark(Integer num, String path) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        this.jsc = new JavaSparkContext(conf);
        this.topNum = jsc.broadcast(num);
        this.inputPath = path;
    }

    public void run() {
        JavaRDD<String> rdd = jsc.textFile(this.inputPath);

        JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> uniqueKeys = wordCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaRDD<SortedMap<Integer, String>> partitions = uniqueKeys.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Integer>>, SortedMap<Integer, String>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                final int N = topNum.getValue();
                SortedMap<Integer, String> topN = new TreeMap<>();
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, Integer> tuple = tuple2Iterator.next();
                    topN.put(tuple._2, tuple._1);
                    if(topN.size() > N) {
                        topN.remove(topN.firstKey());
                    }

                }
                return (Iterator<SortedMap<Integer, String>>) Collections.singletonList(topN);
            }
        });

        SortedMap<Integer, String> finalTopN = partitions.reduce(new Function2<SortedMap<Integer, String>, SortedMap<Integer, String>, SortedMap<Integer, String>>() {

            @Override
            public SortedMap<Integer, String> call(SortedMap<Integer, String> m1, SortedMap<Integer, String> m2) throws Exception {
                final int N = topNum.getValue();
                SortedMap<Integer, String> topN = new TreeMap<Integer, String>();
                for (Map.Entry<Integer, String> entry : m1.entrySet()) {
                    topN.put(entry.getKey(), entry.getValue());
                    if (topN.size() > N) {
                        topN.remove(topN.firstKey());
                    }
                }
                for (Map.Entry<Integer, String> entry : m2.entrySet()) {
                    topN.put(entry.getKey(), entry.getValue());
                    if (topN.size() > N) {
                        topN.remove(topN.firstKey());
                    }
                }
                return topN;
            }
        });
        for (Map.Entry<Integer, String> entry : finalTopN.entrySet()) {
            System.out.println(entry.getKey() + " -- " + entry.getValue());
        }
    }
    public static void main(String[] args) throws Exception {

        init();
        Integer topN = 10;
        String inputPath = "hdfs://192.168.30.128:9000/input/wordcount.txt";
        TopNSpark topNSpark = new TopNSpark(10, inputPath);
        topNSpark.run();

    }

    public static void init() throws Exception {
        String path = new File(".").getCanonicalPath();
        System.getProperties().put("hadoop.home.dir", path);
        new File("./bin").mkdirs();
        new File("./bin/winutils.exe").createNewFile();

    }
}

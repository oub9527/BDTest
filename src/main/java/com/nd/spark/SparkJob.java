package com.nd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.File;
import java.util.*;

/**
 * @author: oubin
 * @date: 2018/9/26 19:00
 * @Description:
 */
public class SparkJob {

    public static void init() throws Exception {
        String path = new File(".").getCanonicalPath();
        System.getProperties().put("hadoop.home.dir", path);
        new File("./bin").mkdirs();
        new File("./bin/winutils.exe").createNewFile();

    }

    public static void main(String[] args) throws Exception {

        init();

        System.out.println(new Date(System.currentTimeMillis()));

        SparkConf conf = new SparkConf().setAppName("WordCount");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);
//        JavaRDD<String> lines = sc.textFile("hdfs://192.168.30.128:9000/input/countFileSmall.txt");
//        lines.coalesce(4);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.replace(" ",",").split(",")).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordCount = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });


        JavaPairRDD<String, Integer> counts = wordCount.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> counts1 = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });

        counts1.sortByKey(false).saveAsTextFile(args[1]);

//        Object[] array = counts1.sortByKey(false).take(5).toArray();
//        for (int i = 0; i < array.length; i++) {
//
//            Tuple2<Integer, String> tuple2 = (Tuple2<Integer, String>) array[i];
//            System.out.println(tuple2._2 + ": " + tuple2._1);
//
//        }
//
        sc.close();

        System.out.println(new Date(System.currentTimeMillis()));

    }
}

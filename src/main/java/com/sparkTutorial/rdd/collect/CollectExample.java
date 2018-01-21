package com.sparkTutorial.rdd.collect;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class CollectExample {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("collect").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // create a list of strings
        List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
        // parallelize takes a list and return an RDD of the same type of input list
        JavaRDD<String> wordRdd = sc.parallelize(inputWords);

        // perform collect action on RDD and return a list of the same type
        List<String> words = wordRdd.collect();

        for (String  word : words) {
            System.out.println(word);
        }
    }
}

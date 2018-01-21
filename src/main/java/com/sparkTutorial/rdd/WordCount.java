package com.sparkTutorial.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // set log level to error
        Logger.getLogger("org").setLevel(Level.ERROR);
        // spaek config app named "wordCounts" that run in a spark embedded instance in our local
        // and will use up to 3 cores of our cpu
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        // spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // load the article ad an RDD (Resilient Distributed Datasets)
        JavaRDD<String> lines = sc.textFile("in/word_count.text");

        // each line as an argument
        // split the line using space wich will return as an array of words of that line (line.split(" "))
        // then we convert the array to a list then to a string iterator Arrays.asList(String[]).iterator()
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // calculate the occurrence of each word and print it out the results
        Map<String, Long> wordCounts = words.countByValue();
        // countByValue() will return the count of each unique value in the input RDD as a map of value and count pairs
        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}

package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLogProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        // run with all cores in local cpu
        SparkConf sparkConf = new SparkConf().setAppName("unionNasaLogs").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // load input files into RDD
        JavaRDD<String> julyFirstLogs = sparkContext.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = sparkContext.textFile("in/nasa_19950801.tsv");

        // using the union of RDDs
        JavaRDD<String> aggregatedLogLines = julyFirstLogs.union(augustFirstLogs);

        // clean logs, remove lines that contain header
        JavaRDD<String> cleanLogLines = aggregatedLogLines.filter(line -> inNotHeader(line));

        // set sample
        JavaRDD<String> sample = cleanLogLines.sample(true, 0.1);

        sample.saveAsTextFile("out/sample_nasa_logs_01.csv");
    }

    private static boolean inNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }
}

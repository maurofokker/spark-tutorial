package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("sumOfNumbers").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/prime_nums.text");

        // RDD from loaded file
        JavaRDD<String> numbers = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        JavaRDD<String> validNumbers = numbers.filter(number -> !number.isEmpty());

        // transform from string to integer
        JavaRDD<Integer> intNumbers = validNumbers.map(number -> Integer.valueOf(number));

        System.out.println("Sum is: " + intNumbers.reduce((x, y) -> x + y));
    }
}

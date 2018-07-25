package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */

        // spark config in 2 local cores
        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");

        // spark context is the main entry point; represent the conn to spark cluster
        // it can be use to create RDDsm accumulators and broadcast variables on the cluster
        JavaSparkContext sc = new JavaSparkContext(conf);

        // load file and create RDD of strings; each of there represent a line
        JavaRDD<String> airports = sc.textFile("in/airports.text");

        // this is a transformation that generates a new RDD of strings
        JavaRDD<String> airportsInUSA = airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));

        // another transformation
        JavaRDD<String> airportsNameAndCityNames = airportsInUSA.map(line -> {
                    String[] splits = line.split(Utils.COMMA_DELIMITER);
                    return StringUtils.join(new String[]{splits[1], splits[2]}, ",");
                }
        );
        // output to new file
        airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text");

    }
}

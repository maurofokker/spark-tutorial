package com.sparkTutorial.pairRdd.filter;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AirportsNotInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */
        // spark config in 2 local cores
        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");

        // spark context is the main entry point; represent the conn to spark cluster
        // it can be use to create RDDsm accumulators and broadcast variables on the cluster
        JavaSparkContext sc = new JavaSparkContext(conf);

        // load file and create RDD of strings; each of there represent a line
        JavaRDD<String> airportsRDD = sc.textFile("in/airports.text");

        // transform into PairRDD
        JavaPairRDD<String, String> airportPairRDD = airportsRDD.mapToPair(getAirportNameAndCountryNamePair());

        // filter by country USA
        JavaPairRDD<String, String> airportsNotInUSA = airportPairRDD.filter(keyValue -> !keyValue._2().equals("\"United States\""));

        airportsNotInUSA.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text");
    }

    public static PairFunction<String, String, String> getAirportNameAndCountryNamePair() {
        return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[1],
                line.split(Utils.COMMA_DELIMITER)[3]);
    }
}

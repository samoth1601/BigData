/* SimpleApp.java */
import org.apache.commons.beanutils.converters.IntegerArrayConverter;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;



public class App {
    /** 1. Load the Foursquare dataset.*/
    final static String datasetRaw = "./src/main/resources/dataset_TIST2015.tsv";
    final static String datasetCitiesRaw = "./src/main/resources/dataset_TIST2015_cities.txt";
    final static SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
    final static JavaSparkContext sc = new JavaSparkContext(conf);
    final static JavaRDD<String> dataset = sc.textFile(datasetRaw);
    final static JavaRDD<String> datasetCities = sc.textFile(datasetCitiesRaw);



    public static void main(String[] args) {

        /**PRØVE FJERNE NOE HEADERGREIER?
        String header = dataset.first();

        JavaRDD<String> datasetNew = dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                String[] splittedLine = s.split("\t");

                return String.join("\t", splittedLine);
            }
        });
         */

        /** 2. Calculate local time for each check-in (UTC time + timezone offset). */

//        JavaRDD<String> localTimeRDD = (convertToLocalTime(dataset));
//        List<String> localTimeStringList = localTimeRDD.take(10);
//        //List<String> localTimeStringList = localTimeRDD.takeOrdered(5);
//        for (String line : localTimeStringList) {
//            System.out.println(line);
//        }

        /** 3.Assign a city and country to each check-in*/


        /** 4. Answer the following questions:*/
        /*
        //todo (a) How many unique users are represented in the dataset?
        float uniqueUsers = countUniqueUsers(dataset);
        System.out.println("Unique users: " + uniqueUsers);

        //todo (b) How many times did they check-in in total?
        float totalSessions = countTotalSessions(dataset);
        System.out.println("Total number of sessions : " + totalSessions);

        //todo (c) How many check-in sessions are there in the dataset?
        float uniqueCheckInSessions = countUniqueSessions(dataset);
        System.out.println("Unique users: " + uniqueUsers);

        //todo (d) How many countries are represented in the dataset?

        //todo (e) How many cities are represented in the dataset?
*/

        /**Calculate lengths of sessions as number of check-ins and provide a histogram
        of these lengths*/
        JavaPairRDD<String,Integer> sessionPairRDD = (countSessionsLength(dataset));
        List<Tuple2<String,Integer>> localTimeStringList = sessionPairRDD.collect();
        //List<String> localTimeStringList = localTimeRDD.takeOrdered(5);
        for (Tuple2<String, Integer> line : localTimeStringList) {
            System.out.println(line);
        }
    }

    private static JavaRDD<String> assignCityAndCountyToEachCheckIn(JavaRDD<String> dataset){
        return dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception{
                String[] splittedLine = s.split("\t");
                //henter latLon
                String lat = splittedLine[5];
                String lon = splittedLine[6];

                //for alle byer, sjekk hvilken som er nærmest.
                //Kanskje kjøre map inni denne igjen?...

                return String.join("\t", splittedLine);
            }
        });
    }


    private static JavaPairRDD<String,Integer> countSessionsLength(JavaRDD<String> dataset){
        return  dataset.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) {
                        String[] splittedLine = x.split("\t");
                        String sessionId = splittedLine[2];
                        return new Tuple2(sessionId, 1); }
                }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer a, Integer b) { return a + b; }
                }).mapToPair(
                new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        Integer tempKey = stringIntegerTuple2._2();
                        return new Tuple2(tempKey, 1);}
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).sortByKey();
    }

    private static long countTotalSessions(JavaRDD<String> dataset){
        return dataset.count();
    }

    private static long countUniqueSessions(JavaRDD<String> dataset){
        return dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception{
                String[] splittedLine = s.split("\t");
                String sessionId = splittedLine[2];
                return sessionId;
            }
        }).distinct().count();
    }

    private static long countUniqueUsers(JavaRDD<String> dataset){
        return dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception{
                String[] splittedLine = s.split("\t");
                String userId = splittedLine[1];
                return userId;
            }
        }).distinct().count();
    }

    private static JavaRDD<String> convertToLocalTime(JavaRDD<String> dataset){
        return dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception{
                String[] splittedLine = s.split("\t");
                String currentDate = splittedLine[3];
                String timeOffset = splittedLine[4];
                String updatedDate = calculateLocalTime(currentDate,timeOffset);
                splittedLine[4] = updatedDate;
                return String.join("\t", splittedLine);
            }
        });
    }

    private static String calculateLocalTime(String dateTime, String stringOffset) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date temp = new Date();
        try {
            temp = df.parse(dateTime);
            Calendar cal = Calendar.getInstance();
            cal.setTime(temp);
            cal.add(Calendar.MINUTE, Integer.parseInt(stringOffset));
            return df.format(cal.getTime());
        } catch (ParseException e) {}
        return temp.toString();
    }
}
/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class App {
    /** 1. Load the Foursquare dataset.*/
    final static String datasetRaw = "./src/main/resources/dataset_TIST2015.tsv";
    final static String datasetCitiesRaw = "./src/main/resources/dataset_TIST2015_cities.txt";
    final static SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
    final static JavaSparkContext sc = new JavaSparkContext(conf);
    final static JavaRDD<String> dataset = sc.textFile(datasetRaw);
    final static JavaRDD<String> datasetCities = sc.textFile(datasetCitiesRaw);

    static Function2 removeHeader= new Function2<Integer, Iterator<String>, Iterator<String>>(){
        @Override
        public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
            if(ind==0 && iterator.hasNext()){
                iterator.next();
                return iterator;
            }else
                return iterator;
        }
    };
    final static JavaRDD<String> datasetNoHeader = dataset.mapPartitionsWithIndex(removeHeader, false);


    public static void main(String[] args) {

        /** TASK 2*/
        task2();


        /** TASK 3 */
        task3();

        /** Task 4 (a) */
        task4a();

        /** Task 4 (b) */
        task4b();

        /** Task 4 (c) */
        task4c();


        /**Calculate lengths of sessions as number of check-ins and provide a histogram
         of these lengths*/
        /*
        JavaPairRDD<String,Integer> sessionPairRDD = (countSessionsLength(dataset));
        List<Tuple2<String,Integer>> localTimeStringList = sessionPairRDD.collect();
        //List<String> localTimeStringList = localTimeRDD.takeOrdered(5);
        for (Tuple2<String, Integer> line : localTimeStringList) {
            System.out.println(line);
        }*/

        /**For sessions with 4 and more check-ins, calculate their distance in kilometers
         (use Haversine formula to compute distance between two pairs of geo.
         coordinates).*/
        //ASSUMING ALREADY SORTED BY DATE!!!
    }


    /****************************************************************************/
    /*************************** TASK 2 *****************************************/
    /** 2. Calculate local time for each check-in (UTC time + timezone offset). */
    /****************************************************************************/
    private static void task2() {
        for (String string : convertToLocalTime(dataset).take(10)) {
            System.out.println(string);
        }
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

    /****************************************************************************/
    /*************************** TASK 3 *****************************************/
    /************** 3.Assign a city and country to each check-in ****************/
    /****************************************************************************/
    private static void task3() {
        for (String string : assignCityAndCountry(datasetNoHeader).take(10)) {
            System.out.println(string);
        }
    }

    static double shortestDist;
    static String shortestCity;
    static String shortestCountry;
    static List<String> cities;

    private static JavaRDD<String> assignCityAndCountry(JavaRDD<String> dataset){
        cities = datasetCities.collect();
        return dataset.map(new Function<String, String>() {
            public String call(String s) throws Exception{
                //For hver checkin, map med dette
                String[] splittedLine = s.split("\t");
                String checkinLat = splittedLine[5];
                String checkinLon = splittedLine[6];
                System.out.println(checkinLat + ", " + checkinLon);


                String[] closestCity = getClosestCity(checkinLat,checkinLon);
                splittedLine[5] = closestCity[0];
                splittedLine[6] = closestCity[1];
                return String.join("\t", splittedLine);
            }
        });
    }


    //HELPER method for finding the closest city to LatLong of checkings
    private static String[] getClosestCity(String latIn, String lonIn) {
        // setting shortestCity to null to get a clean start
        shortestCity = "";
        double checkinLat = Double.parseDouble(latIn);
        double checkinLon = Double.parseDouble(lonIn);
        // For each line (city) check if this city is the closest to the checkin
        for (String cityString : cities)
        {
            String[] splittedLine = cityString.split("\t");

            double lat2 = Double.parseDouble(splittedLine[1]);
            double lon2 = Double.parseDouble(splittedLine[2]);
            double distanceToCity = Haversine.distance(checkinLat, checkinLon, lat2, lon2);

            if (shortestCity == "") {
                shortestCity = splittedLine[0];
                shortestCountry = splittedLine[4];
                shortestDist = distanceToCity;
                //ELSE
            } else if (distanceToCity < shortestDist) {
                shortestCity = splittedLine[0];
                shortestCountry = splittedLine[4];
                shortestDist = distanceToCity;
            }
        }

        String[] toReturn = new String[2];toReturn[0]=shortestCity;toReturn[1]=shortestCountry;
        return toReturn;
    }


    /****************************************************************************/
    /*************************** TASK 4.a) **************************************/
    /********** How many unique users are represented in the dataset? ***********/
    /****************************************************************************/
    private static void task4a(){
        System.out.println("uniqueUsers" + countUniqueUsers(dataset));
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

    /****************************************************************************/
    /*************************** TASK 4.b) **************************************/
    /************* How many times did they check-in in total? *******************/
    /****************************************************************************/
    private static void task4b(){
        System.out.println("Total checkins: " + countTotalSessions(dataset));
    }

    private static long countTotalSessions(JavaRDD<String> dataset){
        return dataset.count();
    }


    /****************************************************************************/
    /*************************** TASK 4.c) **************************************/
    /********** How many check-in sessions are there in the dataset? ************/
    /****************************************************************************/
    private static void task4c(){
        System.out.println("Unique sessions: " + countUniqueSessions(dataset));
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

    /****************************************************************************/
    /*************************** TASK 4.d) **************************************/
    /**********  How many countries are represented in the dataset? ************/
    /****************************************************************************/
    private static void task4d(){
        System.out.println("Unique countries: " + countUniqueCountries(datasetNoHeader));
    }

    private static long countUniqueCountries(JavaRDD<String> dataset) {
        return assignCityAndCountry(dataset).map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] splittedLine = s.split("\t");
                return splittedLine[6];
            }
        }).distinct().count();
    }

    /*************************** TASK 4.e) **************************************/
    /**********  How many cities are represented in the dataset? ************/
    /****************************************************************************/
    private static void task4e(){
        System.out.println("Unique countries: " + countUniqueCities(datasetNoHeader));
    }

    private static long countUniqueCities(JavaRDD<String> dataset) {
        return assignCityAndCountry(datasetNoHeader).mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] splittedLine = s.split("\t");
                return new Tuple2(splittedLine[6], splittedLine[5]);
            }
        }).distinct().count();
    }


    /****************************************************************************/
    /*************************** TASK 5 *****************************************/
    /********** Calculate lengths of sessions as number of check-ins   **********/
    /********** and provide a histogram of these lengths.              **********/
    /****************************************************************************/
    //for histogram.. use som log-scale
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



    /****************************************************************************/
    /*************************** TASK 6 *****************************************/
    /********** For sessions with 4 and more check-ins, *************************/
    /********** calculate their distance in kilometers  *************************/
    /****************************************************************************/
    private static JavaPairRDD<String, Double> computeSessionDistance(JavaRDD<String> dataset){
        // lager KeyValueRDD (Pair RDD med sessionId som key og hele checkinstringen som value)
        return  dataset.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String x) {
                        String[] splittedLine = x.split("\t");
                        String sessionId = splittedLine[2];
                        return new Tuple2(sessionId, x); }
                })
                // Slår sammen alle linjene med samme key og legger values etter hverandre i samme StringSet.
                .aggregateByKey(new HashSet<String>(),
                        new Function2<Set<String>, String, Set<String>>() {
                            @Override
                            public Set<String> call(Set<String> a, String b) {
                                a.add(b);
                                return a;
                            }
                        },
                        new Function2<Set<String>, Set<String>, Set<String>>() {
                            @Override
                            public Set<String> call(Set<String> a, Set<String> b) {
                                a.addAll(b);
                                return a;
                            }
                        })
                // Filter, hvis færre enn 4 checkins, remove
                .filter(new Function<Tuple2<String, Set<String>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Set<String>> stringSetTuple2) throws Exception {
                        return (stringSetTuple2._2().size() >= 4);
                    }
                })

                //For hver sessionId, split stringSettet til checkins, og kalkuler avstanden mellom dem.
                //ASSUMING ALREADY SORTED BY DATE!!!

                .mapValues(new Function<Set<String>, Double>() {
                    @Override
                    public Double call(Set<String> strings) throws Exception {
                        if (strings.size()<4){
                            return 0.0;
                        }
                        double previousLat = 0.0;
                        double previousLon = 0.0;
                        boolean first = true;
                        double distance = 0;
                        for (String checkIn : strings){
                            String[] splittedLine = checkIn.split("\t");
                            if (first){
                                previousLat = Double.parseDouble(splittedLine[5]);
                                previousLon = Double.parseDouble(splittedLine[6]);
                                first = false;
                                continue;
                            }else{
                                double lat = Double.parseDouble(splittedLine[5]);
                                double lon = Double.parseDouble(splittedLine[6]);
                                distance += Haversine.distance(previousLat,previousLon,lat,lon);
                                previousLat=lat;
                                previousLon=lon;
                            }
                        }
                        return distance;
                    }
                });
    }

}

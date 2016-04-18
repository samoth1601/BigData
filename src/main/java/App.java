/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class App {
    public static void main(String[] args) {

        /**
         * 4. utc_time - check-in time on the server represented by a UNIX timestamp.
         5. timezone_offset - timezone offset of the check-in in minutes with respect
         to UTC time.



         */

        /**1. Load the Foursquare dataset.*/
        String logFile = "./src/main/resources/dataset_TIST2015.tsv"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");;
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> logData = sc.textFile(logFile).cache();
        JavaRDD<String> lines = sc.textFile(logFile);
        //JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        //int totalLength = lineLengths.reduce((a, b) -> a + b);


        /** 2. Calculate local time for each check-in (UTC time + timezone offset).*/



        long numAs = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        System.out.println("Lines with a: " + numAs);
    }





}
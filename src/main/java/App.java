/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class App {
    public static void main(String[] args) {

        /**
         * 4. utc_time - check-in time on the server represented by a UNIX timestamp.
         5. timezone_offset - timezone offset of the check-in in minutes with respect
         to UTC time.



         */

        /**1. Load the Foursquare dataset.*/
        String logFile = "C:/Users/jonmartin/Desktop/BigData/Foursquare_data/dataset_TIST2015.tsv"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");;
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> logData = sc.textFile(logFile).cache();
        JavaRDD<String> lines = sc.textFile(logFile);
        //JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        //int totalLength = lineLengths.reduce((a, b) -> a + b);

        //List<String> sortedLines = lines.takeOrdered(lines.count());
        List<String> sortedLines = lines.takeOrdered((int) lines.count()/10);
        for (String line : sortedLines) {
            String[] splittedLine = line.split("\t");
            String dateTime = splittedLine[3];
            String timeOffset = splittedLine[4];
            //System.out.println(time + " " + timeOffset);
            calculateLocalTime(dateTime, timeOffset);
            //System.out.println(dateTime + " " + timeOffset);
        }
        //System.out.println(sortedLines);
        System.out.println("heeeeeeeeeeeey");
        /** 2. Calculate local time for each check-in (UTC time + timezone offset).

        long numAs = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        System.out.println("Lines with a: " + numAs);*/
    }

    private static void calculateLocalTime(String dateTime, String stringOffset) {
        int offset = Integer.parseInt(stringOffset);
        Date temp;
        try {
            temp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateTime);
            System.out.print("TIME IS: ");
            System.out.print(temp);
            Calendar cal = Calendar.getInstance();
            cal.setTime(temp);
            cal.add(Calendar.MINUTE, offset);
            System.out.println("LOCAL TIME IS: ");
            System.out.print(cal.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
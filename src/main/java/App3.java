/* SimpleApp.java */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class App3 {
    public static void main(String[] args) throws IOException {
        String logFile = "./src/main/resources/dataset_TIST2015.tsv"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");;
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> logData = sc.textFile(logFile).cache();
        JavaRDD<String> textFile = sc.textFile(logFile);
        //JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        //int totalLength = lineLengths.reduce((a, b) -> a + b);



        JavaRDD<String> test = textFile.map(new Function<String, String>() {
            public String call(String line) throws Exception{
                String[] values = line.split("\t");
                String currentDate = values[3];
                String timePlus = values[4];

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = simpleDateFormat.parse(currentDate);

                Calendar calendar = Calendar.getInstance(); // creates a new calendar instance
                calendar.setTime(date);   // assigns calendar to given date
                calendar.get(Calendar.HOUR_OF_DAY); // gets hour in 24h format
                calendar.get(Calendar.HOUR);        // gets hour in 12h format
                calendar.get(Calendar.MONTH);
                calendar.add(calendar.MINUTE, Integer.parseInt(timePlus));

                return simpleDateFormat.format(calendar.getTime());
            }
        });

        System.out.println(test);
        System.out.println("yolo");

        List<String> lines = test.takeOrdered(50);
        System.out.println(lines);

        for (String words : lines){
            System.out.println(words);
        }


        //legger de sammenslåtte stringene over i en ny rdd. gjennom å bruke map kanskjeee?
        //rdd.catch ellerno for å vise.

/*
        JavaRDD<String> words = textFile.flatMap(
                new FlatMapFunction<String, String>() { public Iterable<String> call(String s) {
                    System.out.println(Arrays.asList(s.split("\t")[4]));
                    return Arrays.asList(s.split("\t"));
                }});
                */

/*
        JavaRDD<String> linesSplitted = textFile.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")));




        List<String> lines = linesSplitted.takeOrdered(50);
        System.out.println(lines);

        for (String words : lines){
            System.out.println(words);
        }



        System.out.println("yolo");
*/


        /*
        JavaRDD<String> times = textFile.map(return Arrays.asList(s.split(" "))
        //JavaRDD<String> textFile = sc.textFile("hdfs://...");

        */

        /*
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });
        counts.saveAsTextFile("./src/main/resources/results.txt");
        */




    }

    public void mapFunction(){

    }
}
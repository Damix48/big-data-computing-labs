import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class WordCountExample {

  public static void main(String[] args) throws IOException {

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // CHECKING NUMBER OF CMD LINE PARAMETERS
    // Parameters are: num_partitions, <path_to_file>
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    if (args.length != 2) {
      throw new IllegalArgumentException("USAGE: num_partitions file_path");
    }

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // SPARK SETUP
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    SparkConf conf = new SparkConf(true).setAppName("WordCount");
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.setLogLevel("WARN");

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // INPUT READING
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    // Read number of partitions
    int K = Integer.parseInt(args[0]);

    // Read input file and subdivide it into K random partitions
    JavaRDD<String> docs = sc.textFile(args[1]).repartition(K).cache(); // Create RDD

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // SETTING GLOBAL VARIABLES
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    long numdocs, numwords;
    numdocs = docs.count();
    System.out.println("Number of documents = " + numdocs);
    JavaPairRDD<String, Long> wordCounts;
    Random randomGenerator = new Random();

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // STANDARD WORD COUNT with reduceByKey
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    wordCounts = docs.flatMapToPair((document) -> {
      String[] tokens = document.split(" ");
      HashMap<String, Long> counts = new HashMap<>();
      ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

      for (String token : tokens) {
        counts.put(token, counts.getOrDefault(token, 0L) + 1L);
      }

      for (Map.Entry<String, Long> e : counts.entrySet()) {
        pairs.add(new Tuple2<String, Long>(e.getKey(), e.getValue()));
      }

      return pairs.iterator();
    }).reduceByKey((x, y) -> x + y);

    numwords = wordCounts.count();

    System.out.println("Number of words: " + numwords);

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // IMPROVED WORD COUNT (keys in [0,K-1]) with groupByKey
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // IMPROVED WORD COUNT (keys in [0,K-1]) with groupBy
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    wordCounts = docs.flatMapToPair((document) -> {
    });
  }
}
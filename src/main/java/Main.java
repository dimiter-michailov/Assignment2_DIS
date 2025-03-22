import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getRootLogger().setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName(Main.class.getName());
        conf = conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName("2ID70")
                .getOrCreate();

        Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds = Q1(sc, spark);

        Q2(spark);
        Q3(rdds);
        Q4(rdds);
    }

    public static Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> Q1(JavaSparkContext sc, SparkSession spark) {
        JavaRDD<String> patients = null;
        JavaRDD<String> prescriptions = null;
        JavaRDD<String> diagnoses = null;

        // Create views in spark session for SparkSQL

        return new Tuple3<>(patients, prescriptions, diagnoses);
    }

    public static void Q2(SparkSession spark) {
        var q21 = 0;
        System.out.println(">> [q21: " + q21 + "]");

        var q22 = 0;
        System.out.println(">> [q22: " + q22 + "]");

        var q23 = 0;
        System.out.println(">> [q23: " + q23 + "]");
    }

    public static void Q3(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds) {
        var q31 = 0;
        System.out.println(">> [q31: " + q31 + "]");

        var q32 = 0;
        System.out.println(">> [q32: " + q32 + "]");

        var q33 = 0;
        System.out.println(">> [q33: " + q33 + "]");
    }

    public static void Q4(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds) {
        var q4 = 0;
        System.out.println(">> [q4: " + q4 + "]");
    }
}

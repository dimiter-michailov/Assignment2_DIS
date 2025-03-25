import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
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

        // Load the .csv files.
        JavaRDD<String> patients = sc.textFile("data/patients.csv");
        JavaRDD<String> prescriptions = sc.textFile("data/prescriptions.csv");
        JavaRDD<String> diagnoses = sc.textFile("data/diagnoses.csv");

        //Clean the invalid lines from patients.
        patients = patients.filter(line -> {
            String[] attributes = line.split(",", -1);

            if (attributes.length != 4) {
                return false;
            }
        
            String patientId = attributes[0];
            boolean isNumeric = patientId.matches("\\d+");

            if (!isNumeric) {
                return false;
            }
            return true;
        });

        //Split dateOfBirth into 4 separate lineSplit: dateOfBirth + year + month + day
        patients = patients.map(line -> {
            String[] attributes = line.split(",", -1);
        
            //Extract all attributes.
            String patientId = attributes[0];
            String patientName = attributes[1];
            String address = attributes[2];
            String dateOfBirth = attributes[3];
        
            //Split the date attribute.
            String[] datelineSplit = dateOfBirth.split("-");
            String year = datelineSplit[0];
            String month = datelineSplit[1];
            String day = datelineSplit[2];
        
            return patientId + "," + patientName + "," + address + "," + dateOfBirth + "," + year + "," + month + "," + day;
        });

        // Create Row RDD from patients with the extra attributes.
        JavaRDD<Row> patientRows = patients.map(line -> {
            String[] attributes = line.split(",", -1);

            int patientId = Integer.parseInt(attributes[0]);
            String patientName = attributes[1];
            String address = attributes[2];
            String dateOfBirth = attributes[3];

            int birthYear = Integer.parseInt(attributes[4]);
            int birthMonth = Integer.parseInt(attributes[5]);
            int birthDay = Integer.parseInt(attributes[6]);

            return RowFactory.create(patientId, patientName, address, dateOfBirth, birthYear, birthMonth, birthDay);
        });

        // Define the schame for patients.
        StructType patientSchema = new StructType(new StructField[]{
            new StructField("patientId", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("patientName", DataTypes.StringType, false, Metadata.empty()),
            new StructField("address", DataTypes.StringType, false, Metadata.empty()),
            new StructField("dateOfBirth", DataTypes.StringType, false, Metadata.empty()),
            new StructField("birthYear", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("birthMonth", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("birthDay", DataTypes.IntegerType, false, Metadata.empty())
        });

        // Create the DataFrame.
        Dataset<Row> patientDataFrame = spark.createDataFrame(patientRows, patientSchema);
        //Create SQL view.
        patientDataFrame.createOrReplaceTempView("patients");

        //REMOVE THIS LATER
        System.out.println("Valid patient records: " + patientDataFrame.count());

        // Create DataFrame for prescriptions.
        JavaRDD<Row> prescriptionRows = prescriptions.map(line -> {
            String[] attributes = line.split(",", -1);
            int prescriptionId = Integer.parseInt(attributes[0]);
            int medicineId = Integer.parseInt(attributes[1]);
            String dosage = attributes[2];
            return RowFactory.create(prescriptionId, medicineId, dosage);
        });
        StructType prescriptionsSchema = new StructType(new StructField[]{
                new StructField("prescriptionId", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("medicineId", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("dosage", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> dfPrescriptions = spark.createDataFrame(prescriptionRows, prescriptionsSchema);
        dfPrescriptions.createOrReplaceTempView("prescriptions");
    
        //Split date into 4 separate lineSplit: date + year + month + day
        diagnoses = diagnoses.map(line -> {
            String[] attributes = line.split(",", -1);
        
            // Extract attributes
            String patientId = attributes[0];
            String doctorId = attributes[1];
            String date = attributes[2];
            String diagnosisText = attributes[3];
            String prescriptionId = attributes[4];
        
            // Split the date into year + month + day
            String[] datelineSplit = date.split("-");
            String diagYear = datelineSplit[0];
            String diagMonth = datelineSplit[1];
            String diagDay = datelineSplit[2];
        
            return patientId + "," + doctorId + "," + date + "," + diagnosisText + "," + prescriptionId + "," + diagYear + "," + diagMonth + "," + diagDay;
        });

        // Create DataFrame for diagnoses.
        JavaRDD<Row> diagnosisRows = diagnoses.map(line -> {
            String[] attributes = line.split(",", -1);

            int patientId = Integer.parseInt(attributes[0]);
            int doctorId = Integer.parseInt(attributes[1]);
            String date = attributes[2];
            String diagnosis = attributes[3];
            int prescriptionId = Integer.parseInt(attributes[4]);

            String[] datelineSplit = date.split("-");
            int diagYear = Integer.parseInt(datelineSplit[0]);
            int diagMonth = Integer.parseInt(datelineSplit[1]);
            int diagDay = Integer.parseInt(datelineSplit[2]);

            return RowFactory.create(patientId, doctorId, date, diagnosis, prescriptionId, diagYear, diagMonth, diagDay);
        });

        StructType diagnosisSchema = new StructType(new StructField[]{
            new StructField("patientId", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("doctorId", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("date", DataTypes.StringType, false, Metadata.empty()),
            new StructField("diagnosis", DataTypes.StringType, false, Metadata.empty()),
            new StructField("prescriptionId", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("diagYear", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("diagMonth", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("diagDay", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> dfDiagnoses = spark.createDataFrame(diagnosisRows, diagnosisSchema);
        dfDiagnoses.createOrReplaceTempView("diagnoses");

        return new Tuple3<>(patients, prescriptions, diagnoses);
    }    
    
    public static void Q2(SparkSession spark) {
        Dataset<Row> resultQ21 = spark.sql("SELECT COUNT(*) FROM patients WHERE birthYear = 1999");
        long q21 = resultQ21.first().getLong(0);
        System.out.println(">> [q21: " + q21 + "]");

        Dataset<Row> resultQ22 = spark.sql("SELECT date, COUNT(*) AS count FROM diagnoses WHERE diagYear = 2024 GROUP BY date ORDER BY count DESC LIMIT 1");
        String q22 = resultQ22.first().getString(0);
        System.out.println(">> [q22: " + q22 + "]");

        // Count number of medicines per each prescription
        Dataset<Row> countMeds = spark.sql(
            "SELECT d.date, p.prescriptionId, COUNT(p.medicineId) AS medCount " +
            "FROM diagnoses d " +
            "JOIN prescriptions p ON d.prescriptionId = p.prescriptionId " +
            "WHERE d.diagYear = 2024 " +
            "GROUP BY d.date, p.prescriptionId"
        );
        countMeds.createOrReplaceTempView("countMeds");

        // Order by count and leave only top row
        Dataset<Row> resultQ23 = spark.sql(
            "SELECT date, medCount AS count " +
            "FROM countMeds " +
            "ORDER BY count DESC " +
            "LIMIT 1"
        );

        String q23 = resultQ23.first().getString(0);
        System.out.println(">> [q23: " + q23 + "]");
    }

    public static void Q3(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds) {
        JavaRDD<String> patients = rdds._1();
        JavaRDD<String> prescriptions = rdds._2();
        JavaRDD<String> diagnoses = rdds._3();

        JavaRDD<String> filtered = patients.filter(s -> s.split(",", -1)[4].equals("1999"));
        // Map each record to a pair with a key and a count of 1
        JavaPairRDD<String, Integer> pairs = filtered.mapToPair(s -> new Tuple2<>("count", 1));
        // Sum counts
        JavaPairRDD<String, Integer> count = pairs.reduceByKey((a, b) -> a + b);
        var q31 = count.first()._2;
        System.out.println(">> [q31: " + q31 + "]");

        // Filter for diagnoses in 2024, map each record to (date, 1), and count per date.
        JavaPairRDD<String, Integer> dateCounts = diagnoses
            .filter(line -> line.split(",", -1)[5].equals("2024"))
            .mapToPair(line -> {
                String[] lineSplit = line.split(",", -1);
                return new Tuple2<>(lineSplit[2], 1);
            })
            .reduceByKey((a, b) -> a + b);
        // Get (date, count) pair with the maximum count.
        Tuple2<String, Integer> maxDatePair = dateCounts
            .reduce((pair1, pair2) -> pair1._2 > pair2._2 ? pair1 : pair2);
        var q32 = maxDatePair._1;
        System.out.println(">> [q32: " + q32 + "]");

        // Filter diagnoses for 2024 and map to (prescriptionId, date)
        JavaPairRDD<String, String> diagPairs = diagnoses
            .filter(line -> line.split(",", -1)[5].equals("2024"))
            .mapToPair(line -> {
                String[] lineSplit = line.split(",", -1);
                return new Tuple2<>(lineSplit[4], lineSplit[2]);
            });
        // Map prescriptions to (prescriptionId, 1).
        JavaPairRDD<String, Integer> prescPairs = prescriptions
            .mapToPair(line -> {
                String[] lineSplit = line.split(",", -1);
                return new Tuple2<>(lineSplit[0], 1);
            });
        // Join diagnoses and prescriptions on prescriptionId.
        JavaPairRDD<String, Tuple2<String, Integer>> joined = diagPairs.join(prescPairs);
        // Create a key of (date, prescriptionId) with a value 1, then sum.
        JavaPairRDD<Tuple2<String, String>, Integer> groupByDateAndPrescription = joined
            .mapToPair(tuple -> new Tuple2<>(new Tuple2<>(tuple._2()._1, tuple._1), 1))
            .reduceByKey((a, b) -> a + b);
        // Find tuple with maximum medCount.
        Tuple2<Tuple2<String, String>, Integer> maxPair = groupByDateAndPrescription
            .reduce((pair1, pair2) -> pair1._2 > pair2._2 ? pair1 : pair2);
        String q33 = maxPair._1._1;
        System.out.println(">> [q33: " + q33 + "]");
    }

    public static void Q4(Tuple3<JavaRDD<String>, JavaRDD<String>, JavaRDD<String>> rdds) {
        var q4 = 0;
        System.out.println(">> [q4: " + q4 + "]");
    }
}

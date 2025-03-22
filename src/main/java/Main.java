import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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
        JavaRDD<String> patients = sc.textFile("/Users/dimiter.m0108/Downloads/assignment2_data_v2/patients.csv");
        JavaRDD<String> prescriptions = sc.textFile("/Users/dimiter.m0108/Downloads/assignment2_data_v2/prescriptions.csv");
        JavaRDD<String> diagnoses = sc.textFile("/Users/dimiter.m0108/Downloads/assignment2_data_v2/diagnoses.csv");

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

        //Split dateOfBirth into 4 separate parts: dateOfBirth + year + month + day
        patients = patients.map(line -> {
            String[] attributes = line.split(",", -1);
        
            //Extract all attributes.
            String patientId = attributes[0];
            String patientName = attributes[1];
            String address = attributes[2];
            String dateOfBirth = attributes[3];
        
            //Split the date attribute.
            String[] dateParts = dateOfBirth.split("-");
            String year = dateParts[0];
            String month = dateParts[1];
            String day = dateParts[2];
        
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
    
        //Split date into 4 separate parts: date + year + month + day
        diagnoses = diagnoses.map(line -> {
            String[] attributes = line.split(",", -1);
        
            // Extract attributes
            String patientId = attributes[0];
            String doctorId = attributes[1];
            String date = attributes[2];
            String diagnosisText = attributes[3];
            String prescriptionId = attributes[4];
        
            // Split the date into year + month + day
            String[] dateParts = date.split("-");
            String diagYear = dateParts[0];
            String diagMonth = dateParts[1];
            String diagDay = dateParts[2];
        
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

            String[] dateParts = date.split("-");
            int diagYear = Integer.parseInt(dateParts[0]);
            int diagMonth = Integer.parseInt(dateParts[1]);
            int diagDay = Integer.parseInt(dateParts[2]);

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

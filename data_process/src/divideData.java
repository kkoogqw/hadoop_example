
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class divideData {

    private static String orgDatasetPath = "iris.data";
    private static String outBasePath = "";


    public static void main(String[] args) throws IOException {
        buildDataset("test.data", "train.data");

    }

    public static void buildDataset(String testDataName, String trainDataName) throws IOException {

        try {
            FileReader datasetFile = new FileReader(orgDatasetPath);
            BufferedReader datasetBuffer = new BufferedReader(datasetFile);


            File testDataset = new File(outBasePath + testDataName);
            testDataset.createNewFile();
            FileWriter testData = new FileWriter(testDataset);
            BufferedWriter testDataBuffer = new BufferedWriter(testData);

            File trainDataset = new File(outBasePath + trainDataName);
            trainDataset.createNewFile();
            FileWriter trainData = new FileWriter(trainDataset);
            BufferedWriter trainDataBuffer = new BufferedWriter(trainData);

            String lineData = null;
            int index = 0;
            while ((lineData = datasetBuffer.readLine()) != null) {
                if (index % 5 == 0) {
                    testDataBuffer.write(lineData + "\n");
                    testDataBuffer.flush();
                } else {
                    trainDataBuffer.write(lineData + "\n");
                    trainDataBuffer.flush();
                }
                index += 1;
            }
            System.out.println("Finished!");
        } catch (IOException err) {
            err.printStackTrace();
        }

    }

    public static void uploadToHDFS() {
        Configuration conf = new Configuration() {

    }

}

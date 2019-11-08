package gqw.mr.knn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KnnDriver {

    private static String hdfsAddress = "hdfs://localhost:9000";
    private static int K = 3;
    private static int numberOfFeatures = 4;

    public static void runKNN(String dataToBeClassified, int count, String nowTime, String name)
            throws IOException, ClassNotFoundException, InterruptedException {

        String trainDataPath = "/MapReduce/knn-mr/train-data/train.data"; // args[2] -> 训练集文件的路径(HDFS)
        String outputPathBase = "/MapReduce/knn-mr/output-data/classify-result-" + nowTime + "/"; // args[3] ->
                                                                                                  // 分类结果的输出路径(HDFS)

         // 测试数据的维度(特征数量),后面用到

        Configuration conf = new Configuration();
        conf.set("fs.default.name", hdfsAddress);
        conf.set("kValue", Integer.toString(K));
        if (dataToBeClassified != null && dataToBeClassified != "") {
            System.out.print(dataToBeClassified);
            // 分割这一行数据
            String[] features = dataToBeClassified.toString().split("\\ ");
            // 这里录入数据的时候只根据numberOfFeatures的数量读取
            // 后面如果剩余数据就不进行处理
            //
            System.out.println();
            for (int i = 0; i < numberOfFeatures; i++) {
                conf.setFloat("feature" + i, Float.parseFloat(features[i]));
                //
                // System.out.println(features[i]);
            }

            conf.setInt("numberOfFeatures", numberOfFeatures);
            conf.set("name", name);

            Job knnJob = new Job(conf, "KNN on MapReduce");
            knnJob.setJarByClass(KnnDriver.class);

            FileInputFormat.addInputPath(knnJob, new Path(trainDataPath));
            String outputPath = outputPathBase + ("sample-" + Integer.toString(count));
            FileOutputFormat.setOutputPath(knnJob, new Path(outputPath));

            knnJob.setMapperClass(KnnMap.class);
            knnJob.setReducerClass(KnnReduce.class);
            knnJob.setOutputKeyClass(Text.class);
            knnJob.setOutputValueClass(Text.class);
            knnJob.waitForCompletion(true);
            System.out.println();
        } else {
            System.exit(0);
            return;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /**
         * 定义一些Class运行的输入参数
         */

        // 获取当前时间的整数计发作为输出文件的标识(避免多次执行需要定义不同的路径)
        Date date = new Date();
        String nowTime = Long.toString(date.getTime());
        System.out.println("The output floder tag is \" " + nowTime + "\".");

        String nameToBeClassified = "Flowers"; // args[1] -> 要进行分类的name (在输出文件的显示内容)
        String testDataPath = "/MapReduce/knn-mr/todo-data/test.data"; // args[0] -> 测试文件的路径(HDFS)

        Configuration fileConf = new Configuration();
        fileConf.set("fs.default.name", hdfsAddress);
        FileSystem hdfs = FileSystem.get(fileConf);

        // 读取要进行测试的文件
        BufferedReader buffer = new BufferedReader(new InputStreamReader(hdfs.open(new Path(testDataPath))));
        String lineData = null;
        // 没有读到文件末尾一行就继续...
        ArrayList<String> testData = new ArrayList<String>();

        while ((lineData = buffer.readLine()) != null) {
            testData.add(lineData);
        }
        buffer.close();
        hdfs.close();

        int testDataCount = 0;
        for (String nowLineData : testData) {
            System.out.println("Test Sample " + testDataCount + " :");
            runKNN(nowLineData, testDataCount, nowTime, nameToBeClassified);
            testDataCount += 1;
        }
        System.out.println("Finished!");
        System.exit(0);
    }
}
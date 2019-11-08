package gqw.mr.knn;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 这是一个对已有数据集进行划分训练集和测试集的类.
 * 
 * 划分过程将在本地磁盘完成 之后将划分完成的数据集上传到HDFS的指定目录中
 * 
 * 上传的时候要注意，如果没有目录结构，将会执行相应的操作在HDFS上建立对应的目录结构
 * 
 */

public class divideData {
    // HDFS文件系统的网络地址指定
    // 这里由于在伪分布式的环境下开发,所以主机指定为"localhost"
    // 实际运行时需要进行必要的设定
    private static String hdfsAddress = "hdfs://localhost:9000";

    // 指定本地的原始数据地址(默认在当前根目录下)
    // 指定划分好的数据集输出目录
    private static String orgDatasetPath = "knn-iris.data";
    private static String outBasePath = "";

    // 定义HDFS上的文件存放目录
    private static String hdfsTestDataPath = "/MapReduce/knn-mr/todo-data/";
    private static String hdfsTrainDataPath = "/MapReduce/knn-mr/train-data/";

    // 文件名定义
    private static String testFileName = "test.data";
    private static String trainFileName = "train.data";

    public static void main(String[] args) throws IOException {
        buildDirOnHDFS();
        buildDataset(testFileName, trainFileName);
        uploadToHDFS(testFileName, hdfsTestDataPath + testFileName);
        uploadToHDFS(trainFileName, hdfsTrainDataPath + trainFileName);

    }

    // 这个方法在本地执行,将划分好的数据集保存至本地指定目录
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
            // 数据集的划分方法是 % 5 = 0(每五条数据取一条作为测试集)
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

    // 将划分好的数据集(文件)上传到HDFS的方法
    public static void uploadToHDFS(String localFile, String hdfsFile) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", hdfsAddress);

        try {
            FileSystem hdfs = FileSystem.get(conf);

            Path localPath = new Path(localFile);
            Path hdfsPath = new Path(hdfsFile);

            // 如果当前上传的目录已经有文件,则删除原来的文件
            // 将新的进行替换
            if (hdfs.exists(hdfsPath)) {
                hdfs.delete(hdfsPath, true);
                System.out.println("Refresh the data Successfully!");
            }

            hdfs.copyFromLocalFile(localPath, hdfsPath);
            hdfs.close();
        } catch (IOException err) {
            // TODO: handle exception
            err.printStackTrace();
        }
    }

    public static void buildDirOnHDFS() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", hdfsAddress);

        try {
            FileSystem hdfs = FileSystem.get(conf);

            if (!hdfs.exists(new Path("/MapReduce/knn-mr/output-data"))) {
                hdfs.mkdirs(new Path("/MapReduce/knn-mr/output-data"));
            }

            if (!hdfs.exists(new Path("/MapReduce/knn-mr/todo-data"))) {
                hdfs.mkdirs(new Path("/MapReduce/knn-mr/todo-data"));
            }

            if (!hdfs.exists(new Path("/MapReduce/knn-mr/train-data"))) {
                hdfs.mkdirs(new Path("/MapReduce/knn-mr/train-data"));
            }

            System.out.println("Successfully build the dictionary on HDFS!");
        } catch (IOException err) {
            err.printStackTrace();
        }
    }

}

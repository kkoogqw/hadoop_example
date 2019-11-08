package gqw.mapreduce.kmeans;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import javax.security.auth.login.Configuration;

public class datasetProcess {

    private static final String orgDatasetPath = "C:\\Users\\kkoog\\Downloads\\iris.data";
    //private static final String outDataset = "outData.data";

    private static String orgDataset = "/gqw-cloud-vm/k-means/org_dataset/iris.data";
    private static String outDataset = "/gqw-cloud-vm/k-means/org_dataset/";
    private static String outCenterPath = "";

    public String loadInitCenter(Path path, boolean lineCenterChooseType) throws IOException {
        StringBuffer stringBuffer = new StringBuffer();

        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", hostAddress);
        FileSystem hdfs = FileSystem.get(configuration);
        FSDataInputStream dis = hdfs.open(path);

        LineReader in = new LineReader(dis, configuration);
        Text line = new Text();
        if (lineCenterChooseType == false) {
            int c_1 = 0;
            int c_2 = 50;
            int c_3 = 100;

            int lineIndex = 0;
            while(in.readLine(line) > 0 && (lineIndex == c_1 || lineIndex == c_2 ||lineIndex == c_3)) {
                sBuffer.append(line.toString().trim());
                //其中，\t是tab的转义字符 以此来分割不同的质心
                sBuffer.append("\t");
                lineIndex += 1;
            }

        } else {
            int lineIndex = 0;
            while(in.readLine(line) > 0 && lineIndex < 4) {
                sBuffer.append(line.toString().trim());
                //其中，\t是tab的转义字符
                sBuffer.append("\t");
                lineIndex += 1;
            }
        }

        System.out.println("Dataset process finished!");
        return sBuffer.toString().trim();

    }

    public static void datasetAddIndex() {
        try {

            // 读文件
            FileReader datasetFile = new FileReader(orgDatasetPath);
            BufferedReader datasetBuffer = new BufferedReader(datasetFile);

            // 准备写入处理好的数据集
            File newDateset = new File(outDataset);
            newDateset.createNewFile();
            FileWriter newDatasetWriter = new FileWriter(newDateset);
            BufferedWriter outDatasetBUffer = new BufferedWriter(newDatasetWriter);

            // 按行读取,处理后写入文件
            String line = datasetBuffer.readLine();
            int lineIndex = 0;
            while (line != null && !line.equals("")) {
                String[] tempSplit = line.split(",");
                for (int i = 0; i < tempSplit.length; i+=1) {
                    System.out.print(lineIndex + " factor" + i + " is " + tempSplit[i] + "|");
                }
                System.out.println();
                // 写入到缓冲区
                outDatasetBUffer.write(lineIndex+","+line + "\n");
                outDatasetBUffer.flush();

                // 读取下一行数据
                line = datasetBuffer.readLine();
                lineIndex ++;
            }
        } catch (IOException err) {
            err.printStackTrace();
        }
    }



    public static void main(String[] args) {
        datasetAddIndex();
    }

}

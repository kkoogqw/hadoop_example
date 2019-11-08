package gqw.mapreduce.kmeans;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * @author Gao_qw
 * !!!
 * 由于该Java类生成随机序列的随机性过大，在实际执行K-Means算法过程中迭代次数过多
 * 因此次方法已经废弃
 * !!!
 * 现在采用Iris数据集进行K-Means聚类分析测试
 */
public class CreateTrainingDataSet  {
	private static int K = 5;
	private static int DATA_SIZE = 100;

    // 随机生成数据(x,y),作为进行k-means方法的数据集
    private static void writePoint(FileWriter fw) throws IOException {
        float x = new Random().nextFloat() * 10000;
        float y = new Random().nextFloat() * 10000;
//        int x = new Random().nextInt();
//        int y = new Random().nextInt();
        String lineRecordString = String.valueOf(x) + "," + String.valueOf(y) + "\r\n";
        fw.write(lineRecordString);
    }

    private static void CreateDataSet()
    {
        int count=1;
        try {
            // 初始化K个聚类中心
            FileWriter fw1 = new FileWriter("initK");
            while(count <= K) {
                writePoint(fw1);
                count++;
            }
            fw1.close();

            // 生成数据集
            count=1;
            FileWriter fw2 = new FileWriter("kmeans");
            while(count <= DATA_SIZE) {
                writePoint(fw2);
                count++;
            }
            fw2.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        CreateDataSet();
        System.out.println("Finished!");
    }


}
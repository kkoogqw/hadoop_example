package gqw.mr.knn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.sun.org.apache.regexp.internal.REUtil;
import com.sun.org.apache.regexp.internal.recompile;
import com.sun.org.apache.xml.internal.resolver.helpers.PublicId;
import com.sun.prism.impl.BaseResourceFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KnnMap extends Mapper<LongWritable, Text, Text, Text> {
    // 记录当前一条数据的偏移值
    public static long byteOffset = 0;
    // 记录需要分析的特征维度值
    public static Float[] feature = null;
    // 种类记录
    public static String species = null;
    // 测试样本与训练数据集的距离集合
    public static ArrayList<String> distanceList = new ArrayList<String>();
    // 最短距离 默认记为 0
    public static float minDistance = 0;
    // 分析的维度(dim)
    public static int numberOfFeatures = 0;
    // K value
    //public static int K = 10;

    // 距离计算方法
    // 1 - 欧氏距离
    public static float getEulDistance(Float[] trainData, Float[] testData, int dim) {
        float temp = 0;
        for (int i = 0; i < dim; i++) {
            temp += (Math.pow(trainData[i] - testData[i], 2));
        }
        return (float) Math.sqrt(temp);
    }

    // 2 - 曼哈顿距离
    public static float getManhDistance(Float[] trainData, Float[] testData, int dim) {
        float temp = 0;
        for (int i = 0; i < dim; i++) {
            temp += (Math.abs(trainData[i] - testData[i]));
        }
        return temp;
    }
    // 3 - to be continued...
    // TODO more distance Algorithms...

    // 距离获取
    public static float getDistance(Float[] trainData, Float[] testData, int dim, char method) {
        if (method == 'M' || method == 'm') {
            return getManhDistance(trainData, testData, dim);
        } else if (method == 'E' || method == 'e') {
            return getEulDistance(trainData, testData, dim);
        }
        // else if (...)
        else {
            return (float) 0;
        }
    }

    // 重写Mapper的部分函数
    @Override
    // setup() 方法, 根据输入设定分析的维度/载入训练集数据
    public void setup(Context context) throws IOException, InterruptedException {
        numberOfFeatures = context.getConfiguration().getInt("numberOfFeatures", 1);
        feature = new Float[numberOfFeatures];
        for (int i = 0; i < numberOfFeatures; i++) {
            feature[i] = context.getConfiguration().getFloat("feature" + i, 0);
        }
    }

    @Override
    /**
     * 这里的map方法的功能为求取测试数据(未分类的数据)与训练集每一条数据的特定距离,并记录在distanceList 表中,将表传递给下一次使用.
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取的文本数据进行分割
        String[] subFeatures = value.toString().split("\\ ");
        Float[] test = new Float[numberOfFeatures];
        // 文本数据转换为可以计算的类型
        for (int i = 0; i < numberOfFeatures; i++) {
            test[i] = Float.parseFloat(subFeatures[i]);
        }
        // 分类属性记录
        species = subFeatures[numberOfFeatures].replace("\"", "");
        // 将当前行(一条数据)求其与训练集的距离(默认为欧氏距离)
        /**
         * 这里通过修改getDistance()的最后一个参数换不同的距离求解方式
         */
        float dis = getDistance(feature, test, numberOfFeatures, 'E');
        //
        // for (int i = 0; i < numberOfFeatures; ++i) {
        // System.out.println(feature[i] + " " + test[i]);
        // }
        // System.out.println(dis);
        distanceList.add(String.valueOf(dis + " " + species));
        // 一次Map操作胡更新偏移量
        byteOffset = Long.parseLong(key.toString());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

        // for (String s : distanceList) {
        //     System.out.println(s);
        // }
        /**
         * 后续有空自己写一个qSort()排序方法... distanceList = qSort(distanceList);
         * 这里先用一个比较低端的冒泡排序替代一下...
         */
        // Collections.sort(distanceList);
        for (int i = 0; i < distanceList.size() - 1; i++) {
            for (int j = 0; j < distanceList.size() - 1 - i; j++) {
                float a = Float.parseFloat(distanceList.get(j).split(" ")[0]);
                float b = Float.parseFloat(distanceList.get(j + 1).split(" ")[0]);
                if (a > b) {
                    String temp = distanceList.get(j);
                    distanceList.set(j, distanceList.get(j + 1));
                    distanceList.set(j + 1, temp);
                }
            }
        }

        int index = 0;
        int K = context.getConfiguration().getInt("kValue", 1);
        // 这里根据需要获取的前n个种类(距离最近的)设定分类结果
        String[] classifySpecies = new String[K];
        String tempString = "";
        for (int i = 0; i < K; i++) {
            tempString = distanceList.get(i);
            String tempSpecie = String.valueOf(tempString.replaceAll("[\\d.]", ""));
            classifySpecies[index] = tempSpecie;
            index += 1;
        }
        Arrays.sort(classifySpecies);
        /**
         * !!!
         * 这是一个坑...
         * 如果不对这个List进行清空操作
         * 只是继续进行 new 操作
         * MapReduce过程此部分的内存不会清空
         * 导致下一条数据会包含上一条数据的信息
         * 
         * 解决之前只能对第一条数据有效...
         */
        distanceList.clear();
        // 这里划定分类标准
        /**
         * 计算获得前k个最近值的样本类型
         * 那么就将这个测试样本归属这个类别
         */
        for (int i = 0; i < classifySpecies.length - 1; i++) {
            if (classifySpecies[i].equals(classifySpecies[i + 1])) {
                // 这里更改<key, value>的键值对传递给Reduce过程
                context.write(new Text("1"), new Text(classifySpecies[i]));
                break;
            }
        }
    }

}
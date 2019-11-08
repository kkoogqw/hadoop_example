package gqw.mr.knn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KnnReduce extends Reducer<Text, Text, Text, Text> {
    String classifyResult = null;
    /**
     * 这里重写 Reducer中的部分函数
     * 这里的接上对KnnMap类中的设定结果进行最后的处理
     */
    @Override
    public void setup(Context context) {
        // 分类结果对应训练数据的"name"标签
        // "name"标签在KnnDriver中设定
        classifyResult = String.valueOf(context.getConfiguration().get("name"));
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 利用一个Hash表,记录根据Map过程的[种类<-->数量]的对应关系
        HashMap<String, Integer> classifyMap = new HashMap<String, Integer>();
        String maxKey = null;
        int maxValue = -1;
        // 迭代器遍历value值,构建一个Hash表
        for (Text value:values) {
            /**
             * 如果当前value不存在,存入到对应的Hash表并记录为1
             * 如果存在当前value(之前出现过),则在原啦的基础上+1(出现次数)
             */
        	//System.out.println(value.toString());
            if (!classifyMap.containsKey(value.toString())) {
                classifyMap.put(value.toString(), 1);
                //
                //System.out.println(value.toString() + " first " + classifyMap.get(value.toString()));
            } else {
                classifyMap.put(value.toString(), classifyMap.get(value.toString()) + 1);
                //System.out.println(value.toString() + " not " + classifyMap.get(value.toString()));
            }
        }

        // 遍历得到的Hash表,不断更新得到出现次数最多的value值
        for (Entry<String, Integer> entry : classifyMap.entrySet()){
            if (entry.getValue() > maxValue) {
                maxKey = entry.getKey();
                maxValue = entry.getValue();
            }
        }
        // 将结果写入到Map-reduce的上下文中
        context.write(null, new Text(classifyResult + " is the speice of <" + maxKey + " >\n"));
        System.out.print("The result of classifing is: " + maxKey);
    }
}
package gqw.mapreduce.kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Run {

	private static String FLAG = "KCLUSTER";
	// 设定HDFS文件地址
	private static String hostAddress = "hdfs://localhost:9000";
	// 设定最大迭代次数(防止不收敛时,陷入无限迭代过程,占用计算资源)
	private static final int MAX_INDEX = 50;
	// 数据集目录
	private static String inputDataPath = "/gqw-cloud-vm/k-means/input_data/kmeans";
	// 初始质心路径
	private static String initCenterPath = "/gqw-cloud-vm/k-means/center_data/initK";
	// 输出目录
	private static String outputPath = "/gqw-cloud-vm/k-means/output_data/";


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		System.out.println("Started!");
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", hostAddress);

		Path kMeansPath = new Path(initCenterPath); // 初始的质心文件
		Path samplePath = new Path(inputDataPath); // 样本文件
		// 加载聚类中心文件
		Center center = new Center();
		String centerString = center.loadInitCenter(kMeansPath);
//		System.out.println("test1");
		System.out.println("Read init center-data finished!");
		
		// 迭代次数记录
		int index = 0;
//		while (index < MAX_INDEX) {
		while (true) {
			configuration = new Configuration();
			configuration.set("fs.default.name", hostAddress);
			configuration.set(FLAG, centerString); // 将聚类中心的字符串放到configuration中

			kMeansPath = new Path(outputPath + index); // 本次迭代的输出路口，也是下一次质心的读取路径
			//System.out.println("test2");

			// 判断输出路径是否存在，如果存在则删除
			FileSystem hdfs = FileSystem.get(configuration);
			//if (hdfs.exists(kMeansPath))
				//hdfs.delete(kMeansPath, true);
			//System.out.println("test3");

			// do k-means
			Job job = Job.getInstance(configuration, "kmeans" + index);
			job.setJarByClass(Run.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, samplePath);
			FileOutputFormat.setOutputPath(job, kMeansPath);
			job.waitForCompletion(true);
			//System.out.println("test4");

			/*
			 * 获取自定义center大小，如果等于质心的大小，说明质心已经不会发生变化了，则程序停止迭代
			 */
			long counter = job.getCounters().getGroup("myCounter").findCounter("kmeansCounter").getValue();
			if (counter == Center.k) {
				break;
				
			}
			/* 重新加载质心 */
			center = new Center();
			//System.out.println("test5");
			centerString = center.loadCenter(kMeansPath);
			index++;
			System.out.println("Loop " + index + " finished!");
		}
		System.out.println("K-means finished!");
		System.exit(0);
	}

}

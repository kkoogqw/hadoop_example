## 基于 MapReduce 的KNN算法实现 说明

---

@ author - Gao Qian wen (16338002)
@ Major - SDCS Information Security
@ Date - 2019/6/15

* 开发环境：
    * Hadoop version2.6.5；
    * JDK 1.8_211;
    * Eclipse-JEE 2019.3;
    * Ubuntu 18.10 (64 bit)

### 项目目录结构与代码文件说明

#### Project 目录

这里是对工程目录中的一些文件说明，方便您的查看。

* /project
  * /knn_mr $\rightarrow$ 工程目录
    * /bin $\rightarrow$ 存放编译后的字节码文件(.class)
      * /gqw.mr.knn
        * divideData.class
        * KnnMap.class
        * KnnReduce.class
        * KnnDriver.class
    * /src $\rightarrow$ 源代码文件
      * /gqw.mr.knn $\rightarrow$ 工程建立的Java包
        * divideData.java $\rightarrow$ **对数据集进行处理的类**
        * KnnMap.java $\rightarrow$ **Mapper过程类**
        * KnnReduce.java $\rightarrow$ **Reducer过程类**
        * KnnDriver.java $\rightarrow$ **Driver控制器，KNN程序执行入口类**
  * Report.pdf $\rightarrow$ 课程设计报告
  * README.md/pdf $\rightarrow$ README文档

#### HDFS 目录结构

* /HDFS_ROOT $\rightarrow$ HDFS 根目录
  * MapReduce $\rightarrow$ MR 项目目录
    * knn-mr $\rightarrow$ KNN程序工作目录
      * todo-data $\rightarrow$ 存放需要分类的数据文件
        * test.data $\rightarrow$ **要进行分类的数据集**
      * trian-data $\rightarrow$ 存放已知类别的数据文件
        * train.data $\rightarrow$ **已知类别的数据集**
      * output-data $\rightarrow$ 分类结果输出目录
        * classify-result-[time stamp 0] $\rightarrow$ 某一个分类作业的输出结果
          * sample-0 $\rightarrow$ **第一个样本的分类结果**
            * _SUCCESS $\rightarrow$ 过程文件
            * part-r-00000 $\rightarrow$ **结果显示**
          * sample-1 $\rightarrow$ 第二个样本的分类结果
          * ...
        * classify-result-[time stamp x]
        * classify-result-[time stamp ...]



### 代码修改与参数配置

为了使代码能够在您配置的环境下运行，您可能需要对源代码进行如下的一些参数修改：

您主要需要修改的源代码文件有两个：
1. divideData.java
``` java
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
        ... ...
    }
    public static void buildDataset(String testDataName, String trainDataName) throws IOException {
        ... ...
    }
    public static void buildDirOnHDFS() throws IOException {
        ... ...
    }
}
```

以上文件您需要修改的就是类中的一些`private static String`类型的变量，其对应的功能分别在上面的注释已经给出。**实际上，如果您在`main`函数中通过执行`buildDirOnHDFS()`方法构建HDFS上的对应目录, 那么您只需要修改前三个变量的值即可, 这是因为后面的路径和文件名设定均为按照上述方法命名的.**

**重要的是, 必须要将您要测试的数据集存放到指定的目录下;**

**对于您要测试的数据集, 其文本格式必须按照一行一个样本的格式进行存储,在一行中,应该有如下的形式:**

$$feature_1\ feature_2\ ...\ feature_n\ "name\ of\ class"$$

**也就是说,每个特征必须要以数字的形式呈现, 不同的特征值要用空格分开,最后的归属类别应该用引号包括.**


2. KnnDriver.java

``` java
public class KnnDriver {

    private static String hdfsAddress = "hdfs://localhost:9000";
    private static int K = 3;
    private static int numberOfFeatures = 4;

    ... ... 
}
```

上面的三个参数分别对应的是HDFS文件的网络地址和端口/你要设定的K值/每个样本包含的特征数量.
如果您在divideData.java中执行了`buildDirOnHDFS()`,那么后面的路径将不需要进行修改就可以直接运行.

### 如何执行代码

如果您已经设置好源代码中的所有参数, 您需要确保Hadoop服务开启后, 先执行divideData.java, 然后执行KnnDriver.java即可在控制台看到相关显示,并得到HDFS中的结果.
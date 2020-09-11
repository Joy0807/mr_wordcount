import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountRunner{
    //把业务逻辑相关的信息（哪个是mapper，哪个是reducer，要处理的数据在哪里，输出的结果放哪里……）描述成一个job对象
    //把这个描述好的job提交给集群去运行

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建一个Configuration实体类对象
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
        // 指定我这个job所在的jar包
        // wcjob.setJar("/home/hadoop/wordcount.jar");
        wcjob.setJarByClass(WordCountRunner.class);

        wcjob.setMapperClass(WordCountMapper.class);
        wcjob.setReducerClass(WordCountReducer.class);

        //设置我们的业务逻辑Mapper 类的输出 key 和 value  的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(LongWritable.class);

        //设置我们的业务逻辑 Reducer 类的输入key 和 value 的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(LongWritable.class);

        long startTime=System.currentTimeMillis();   //获取开始时间


        //指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob,"C:\\Users\\DEll\\Desktop\\test.txt");
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, new Path("E:\\result"));

        // 向yarn集群提交这个job
        boolean res = wcjob.waitForCompletion(true);
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println(res?0:1);
        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");


    }
}
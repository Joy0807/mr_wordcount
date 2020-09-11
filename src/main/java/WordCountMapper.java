//首先要定义四个泛型的类型
//keyin:  LongWritable    valuein: Text
//keyout: Text            valueout:IntWritable

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    //map  方法的生命周期:  框架每传一行数据就被调用一次
    //key :  这一行的起始点在文件中的偏移量
    //value : 这一行的内容

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
/**
 * 代码中  key   是行首字母的【偏移量】-->无规律可言,行首字母到所有内容最前端的
 *        value  是一行真正的数据
 */

        //1.将Text类型的value  转换成 string
        String datas = value.toString();

        //2.将这一行用 " " 切分出各个单词
        String[] words = datas.split(" ");

        //3.遍历数组,输出<单词,1>【一个单词输出一次】
        for (String word : words) {

            //输出数据
            //context   上下文对象
            context.write(new Text(word),new LongWritable(1));

        }

    }

}
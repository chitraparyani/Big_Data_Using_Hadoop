package flights_by_yr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class YearMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

//    hadoop datatype
        Text word = new Text();
        IntWritable one = new IntWritable(1);

@Override
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens= line.split(",");

        if(tokens[0].equals("Year"))
                return;

        word.set(tokens[0]);
        context.write(word,one);

        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        }
}

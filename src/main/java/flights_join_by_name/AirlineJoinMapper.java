package flights_join_by_name;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirlineJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    //    hadoop datatype
    Text word = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens= line.split(",");

        if(tokens[0].equals("Code"))
            return;

        word.set(tokens[0]);
        context.write(word,new Text("A" + tokens[1]));

        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
    }

}

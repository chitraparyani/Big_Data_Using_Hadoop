package flights_binning_by_month;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class BinningMonthMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

//    hadoop datatype
        private MultipleOutputs<Text, NullWritable> multipleOutputs = null;

        @Override
        protected void setup(Context context) {
                multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
        }

@Override
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens= line.split(",");

        if(tokens[0].equals("Year"))
                return;

        int month = Integer.parseInt(tokens[1]);

        if(month == 10){
                multipleOutputs.write("bins", value,NullWritable.get(),"oct");
        }else if(month == 11){
                multipleOutputs.write("bins", value, NullWritable.get(), "Nov");
        }else if(month == 12){
                multipleOutputs.write("bins", value, NullWritable.get(), "Dec");
        }else {
                multipleOutputs.write("bins", value, NullWritable.get(), "Others");
        }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
                multipleOutputs.close();
        }
}

package flight_secsort_by_delay;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        String[] tokens = line.split(",");
        String src = null;
        String dest = null;
        int arrivalDelay = 0;

        try {

            src = tokens[16];
            dest = tokens[17];

//            if(Integer.parseInt(tokens[14]) > 15){
//                arrivalDelay += 1;
//            }

            arrivalDelay = Integer.parseInt(tokens[14]);

        } catch (Exception e) {

        }

        if (src != null && dest != null) {

            CompositeKeyWritable outKey = new CompositeKeyWritable(src, dest, arrivalDelay);

            context.write(outKey, NullWritable.get());

        }
    }

}


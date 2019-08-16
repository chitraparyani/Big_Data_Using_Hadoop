package flight_analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlightReducer extends Reducer<Text, DelayCancelTuple, Text, DelayCancelTuple> {

    // just like in mongoDB values is iterable
    @Override
    protected void reduce(Text key, Iterable<DelayCancelTuple> values, Reducer<Text, DelayCancelTuple, Text, DelayCancelTuple>.Context context) throws IOException, InterruptedException {

        int departureDelay = 0;
        int cancelled = 0;
        int diverted = 0;
        int whetherDelay = 0;

        for (DelayCancelTuple v : values) {
            departureDelay += v.getDepartureDelay();
            cancelled += v.getCancelled();
            diverted += v.getDiverted();
            whetherDelay += v.getWhetherDelay();
            // can we use this--  Integer.parseInt(v.toString());
        }

        context.write(key, new DelayCancelTuple(departureDelay, cancelled, diverted, whetherDelay));

        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
    }
}

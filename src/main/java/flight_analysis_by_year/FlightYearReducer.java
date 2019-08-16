package flight_analysis_by_year;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlightYearReducer extends Reducer<Text, DelayCancelTuple, Text, DelayCancelTuple> {

    // just like in mongoDB values is iterable
    @Override
    protected void reduce(Text key, Iterable<DelayCancelTuple> values, Context context) throws IOException, InterruptedException {

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

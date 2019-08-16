package flight_analysis_from_des;

import flight_analysis_from_src.DepartureDelayTuple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlightDestReducer extends Reducer<Text, ArrivalDelayTuple, Text, ArrivalDelayTuple> {

    // just like in mongoDB values is iterable
    @Override
    protected void reduce(Text key, Iterable<ArrivalDelayTuple> values, Context context) throws IOException, InterruptedException {

      int totalFlights = 0;
      int arrivalDelay = 0;


        for (ArrivalDelayTuple v : values) {

            totalFlights += v.getTotalFlights();
            arrivalDelay += v.getArrivalDelay();
            // can we use this--  Integer.parseInt(v.toString());
        }

        context.write(key, new ArrivalDelayTuple(totalFlights, arrivalDelay, ((double) arrivalDelay/totalFlights)*100));

        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
    }
}

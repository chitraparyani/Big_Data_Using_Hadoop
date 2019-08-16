package flight_analysis_from_src;

import flight_analysis_by_year.DelayCancelTuple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlightSourceReducer extends Reducer<Text, DepartureDelayTuple, Text, DepartureDelayTuple> {

    // just like in mongoDB values is iterable
    @Override
    protected void reduce(Text key, Iterable<DepartureDelayTuple> values, Context context) throws IOException, InterruptedException {

      int totalFlights = 0;
      int delayedFlights = 0;
      int percentDelayedFlights = 0;

        for (DepartureDelayTuple v : values) {

            totalFlights += v.getTotalFlights();
            delayedFlights += v.getDelayedFlights();
            // can we use this--  Integer.parseInt(v.toString());
        }

        context.write(key, new DepartureDelayTuple(totalFlights, delayedFlights, ((double) delayedFlights/totalFlights)*100));

        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
    }
}

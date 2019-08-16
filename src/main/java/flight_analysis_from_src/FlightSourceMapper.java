package flight_analysis_from_src;

import flight_analysis_by_year.DelayCancelTuple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightSourceMapper extends Mapper<LongWritable, Text, Text, DepartureDelayTuple> {

//    hadoop datatype
        Text word = new Text();
        DepartureDelayTuple departureDelayTuple = new DepartureDelayTuple();

@Override
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens= line.split(",");

        if(tokens[0].equals("Year"))
                return;

        word.set(tokens[16]);
        try {
                if (Math.abs(Integer.parseInt(tokens[15])) > 10) {
                        departureDelayTuple.setDelayedFlights(1);
                } else {
                        departureDelayTuple.setDelayedFlights(0);
                }
        }catch(Exception e){
                departureDelayTuple.setDelayedFlights(0);
        }

        departureDelayTuple.setTotalFlights(1);
        departureDelayTuple.setPercentDelayedFlights(0);

        context.write(word,departureDelayTuple);

        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        }
}

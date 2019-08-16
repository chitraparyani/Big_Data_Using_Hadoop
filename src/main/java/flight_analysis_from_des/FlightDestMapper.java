package flight_analysis_from_des;

import flight_analysis_from_src.DepartureDelayTuple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightDestMapper extends Mapper<LongWritable, Text, Text, ArrivalDelayTuple> {

//    hadoop datatype
        Text word = new Text();
        ArrivalDelayTuple arrivalDelayTuple = new ArrivalDelayTuple();

@Override
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens= line.split(",");

        if(tokens[0].equals("Year"))
                return;

        word.set(tokens[16]);
        try {
                if (Math.abs(Integer.parseInt(tokens[14])) > 10) {
                        arrivalDelayTuple.setArrivalDelay(1);
                } else {
                        arrivalDelayTuple.setArrivalDelay(0);
                }
        }catch(Exception e){
                arrivalDelayTuple.setArrivalDelay(0);
        }

        arrivalDelayTuple.setTotalFlights(1);
        arrivalDelayTuple.setPercentArrivalDelay(0);

        context.write(word,arrivalDelayTuple);

        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        }
}

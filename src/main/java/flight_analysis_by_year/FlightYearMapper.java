package flight_analysis_by_year;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightYearMapper extends Mapper<LongWritable, Text, Text, DelayCancelTuple> {

//    hadoop datatype
        Text word = new Text();
        DelayCancelTuple delayCancelTuple = new DelayCancelTuple();

@Override
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens= line.split(",");

        if(tokens[0].equals("Year"))
                return;

        word.set(tokens[0]);
        try {
                if (Math.abs(Integer.parseInt(tokens[15])) > 10) {
                        delayCancelTuple.setDepartureDelay(1);
                } else {
                        delayCancelTuple.setDepartureDelay(0);
                }
        }catch(Exception e){
                delayCancelTuple.setDepartureDelay(0);
        }

        delayCancelTuple.setCancelled(Integer.parseInt(tokens[21]));
        delayCancelTuple.setDiverted(Integer.parseInt(tokens[23]));

        try {
                int val=Integer.parseInt(tokens[25]);
                if(val > 0)
                        delayCancelTuple.setWhetherDelay(1);
                else
                        delayCancelTuple.setWhetherDelay(0);
        }catch (Exception e){
                delayCancelTuple.setWhetherDelay(0);
        }

        context.write(word,delayCancelTuple);

        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        }
}

package flights_join_by_name;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class FlightJoinReducer extends Reducer<Text, Text, Text, Text> {

    private static final Text Empty_Text = new Text("");
    private Text tmp = new Text();

    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();

    private String joinType = null;

    public void setup(Context context){
        // Get the type of join from configuration
        //joinType = context.getConfiguration().get("join.type");

        joinType = "inner";
    }

    // just like in mongoDB values is iterable
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // clear the list out
        listA.clear();
        listB.clear();

        while (values.iterator().hasNext()){

            tmp = values.iterator().next();

            if(tmp.charAt(0) == 'A'){
                listA.add(new Text(tmp.toString().substring(1)));
            }else if(tmp.charAt(0) == 'B'){
                listB.add(new Text(tmp.toString().substring(1)));
            }
        }

        executeJoinLogic(context);



       // context.write(key, new IntWritable(sum));

        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
    }

    private void executeJoinLogic(Context context) throws IOException, InterruptedException {

        if(joinType.equalsIgnoreCase("inner")){
            // if both the list are not empty, join listA and listB

            if(!listA.isEmpty() && !listB.isEmpty()){
                for(Text A : listA){
                    for(Text B : listB){
                        context.write(A, B);
                    }
                }
            }
        }

    }
}

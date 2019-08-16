package flights_join_by_name;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlightJoinMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{


        Configuration conf = new Configuration();
        // Create a new Job
        Job job = Job.getInstance(conf,"wordcount");
        job.setJarByClass(FlightJoinMain.class);

        // Specify various job-specific parameters
        job.setJobName("myjob");


//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, AirlineJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, FlightJoinMapper.class);

        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);

//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
//
//        job.setMapperClass(FlightJoinMapper.class);
         job.setReducerClass(FlightJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Submit the job, then poll for progress until the job is complete
        System.exit(job.waitForCompletion(true)?0:1);

    }
}

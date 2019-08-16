package flight_secsort_by_delay;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<CompositeKeyWritable, NullWritable> {

    @Override
    public int getPartition(CompositeKeyWritable key, NullWritable value, int numPartitions) {

        return key.getSrc().hashCode()%numPartitions;

    }

}

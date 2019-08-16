package flight_secsort_by_delay;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortComparator extends WritableComparator {

    protected SecondarySortComparator() {
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        CompositeKeyWritable ckw1 = (CompositeKeyWritable) a;
        CompositeKeyWritable ckw2 = (CompositeKeyWritable) b;

        int result = ckw1.getSrc().compareTo(ckw2.getSrc());

        if (result == 0) {
            result =  ckw2.getArrivalDelay() - ckw1.getArrivalDelay();
        }

        return result;
    }
}

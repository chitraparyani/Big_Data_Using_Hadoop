package flight_secsort_by_delay;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

    public GroupComparator() {
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(Object a, Object b) {

        CompositeKeyWritable ck1 = (CompositeKeyWritable)a;
        CompositeKeyWritable ck2 = (CompositeKeyWritable)b;

        return ck1.getSrc().compareTo(ck2.getSrc());
    }
}

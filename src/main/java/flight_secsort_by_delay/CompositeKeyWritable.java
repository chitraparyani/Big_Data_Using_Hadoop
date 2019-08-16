package flight_secsort_by_delay;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {

    private String src;
    private String dest;
    private int arrivalDelay;

    public CompositeKeyWritable() {
    }

    public CompositeKeyWritable(String src, String dest, int arrivalDelay) {
        this.src = src;
        this.dest = dest;
        this.arrivalDelay = arrivalDelay;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public int getArrivalDelay() {
        return arrivalDelay;
    }

    public void setArrivalDelay(int arrivalDelay) {
        this.arrivalDelay = arrivalDelay;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(src);
        dataOutput.writeUTF(dest);
        dataOutput.writeInt(arrivalDelay);
    }

    public void readFields(DataInput dataInput) throws IOException {

        src = dataInput.readUTF();
        dest = dataInput.readUTF();
        arrivalDelay = dataInput.readInt();
    }

    public int compareTo(CompositeKeyWritable o) {

        int result = this.src.compareTo(o.src);

        if(result == 0)
            return o.arrivalDelay - this.arrivalDelay;

        return result;
    }

    @Override
    public String toString() {
        return "CompositeKeyWritable{" +
                "src='" + src + '\'' +
                ", dest='" + dest + '\'' +
                ", arrivalDelay=" + arrivalDelay +
                '}';
    }
}

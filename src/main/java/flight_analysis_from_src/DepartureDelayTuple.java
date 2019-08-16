package flight_analysis_from_src;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DepartureDelayTuple implements Writable {

    private int totalFlights;
    private int delayedFlights;
    private double percentDelayedFlights;

    public DepartureDelayTuple() {
    }

    public DepartureDelayTuple(int totalFlights, int delayedFlights, double percentDelayedFlights) {
        this.totalFlights = totalFlights;
        this.delayedFlights = delayedFlights;
        this.percentDelayedFlights = percentDelayedFlights;
    }

    public int getTotalFlights() {
        return totalFlights;
    }

    public void setTotalFlights(int totalFlights) {
        this.totalFlights = totalFlights;
    }

    public int getDelayedFlights() {
        return delayedFlights;
    }

    public void setDelayedFlights(int delayedFlights) {
        this.delayedFlights = delayedFlights;
    }

    public double getPercentDelayedFlights() {
        return percentDelayedFlights;
    }

    public void setPercentDelayedFlights(double percentDelayedFlights) {
        this.percentDelayedFlights = percentDelayedFlights;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(totalFlights);
        dataOutput.writeInt(delayedFlights);
        dataOutput.writeDouble(percentDelayedFlights);

    }

    public void readFields(DataInput dataInput) throws IOException {

        totalFlights = dataInput.readInt();
        delayedFlights = dataInput.readInt();
        percentDelayedFlights = dataInput.readDouble();

    }

    @Override
    public String toString() {
        return "DepartureDelayTuple{" +
                "totalFlights=" + totalFlights +
                ", delayedFlights=" + delayedFlights +
                ", percentDelayedFlights=" + percentDelayedFlights +
                '}';
    }
}

package flight_analysis_from_des;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ArrivalDelayTuple implements Writable {

    private int totalFlights;
    private int arrivalDelay;
    private double percentArrivalDelay;

    public ArrivalDelayTuple() {
    }

    public ArrivalDelayTuple(int totalFlights, int arrivalDelay, double percentArrivalDelay) {
        this.totalFlights = totalFlights;
        this.arrivalDelay = arrivalDelay;
        this.percentArrivalDelay = percentArrivalDelay;
    }


    public int getTotalFlights() {
        return totalFlights;
    }

    public void setTotalFlights(int totalFlights) {
        this.totalFlights = totalFlights;
    }

    public int getArrivalDelay() {
        return arrivalDelay;
    }

    public void setArrivalDelay(int arrivalDelay) {
        this.arrivalDelay = arrivalDelay;
    }

    public double getPercentArrivalDelay() {
        return percentArrivalDelay;
    }

    public void setPercentArrivalDelay(double percentArrivalDelay) {
        this.percentArrivalDelay = percentArrivalDelay;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(totalFlights);
        dataOutput.writeInt(arrivalDelay);
        dataOutput.writeDouble(percentArrivalDelay);
    }

    public void readFields(DataInput dataInput) throws IOException {

        totalFlights = dataInput.readInt();
        arrivalDelay = dataInput.readInt();
        percentArrivalDelay = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "ArrivalDelayTuple{" +
                "totalFlights=" + totalFlights +
                ", arrivalDelay=" + arrivalDelay +
                ", percentArrivalDelay=" + percentArrivalDelay +
                '}';
    }
}

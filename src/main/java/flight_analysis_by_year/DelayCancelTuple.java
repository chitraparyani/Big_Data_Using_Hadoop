package flight_analysis_by_year;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DelayCancelTuple implements Writable  {

    private int departureDelay;
    private int cancelled;
    private int diverted;
    private int whetherDelay;

    public DelayCancelTuple() {
        super();
    }

    public DelayCancelTuple(int departureDelay, int cancelled, int diverted, int whetherDelay) {
        this.departureDelay = departureDelay;
        this.cancelled = cancelled;
        this.diverted = diverted;
        this.whetherDelay = whetherDelay;
    }

    public int getDepartureDelay() {
        return departureDelay;
    }

    public void setDepartureDelay(int departureDelay) {
        this.departureDelay = departureDelay;
    }

    public int getCancelled() {
        return cancelled;
    }

    public void setCancelled(int cancelled) {
        this.cancelled = cancelled;
    }

    public int getDiverted() {
        return diverted;
    }

    public void setDiverted(int diverted) {
        this.diverted = diverted;
    }

    public int getWhetherDelay() {
        return whetherDelay;
    }

    public void setWhetherDelay(int whetherDelay) {
        this.whetherDelay = whetherDelay;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(departureDelay);
        dataOutput.writeInt(cancelled);
        dataOutput.writeInt(diverted);
        dataOutput.writeInt(whetherDelay);
    }

    public void readFields(DataInput dataInput) throws IOException {

        departureDelay = dataInput.readInt();
        cancelled = dataInput.readInt();
        diverted = dataInput.readInt();
        whetherDelay = dataInput.readInt();

    }

    @Override
    public String toString() {
        return "DelayCancelTuple{" +
                "departureDelay=" + departureDelay +
                ", cancelled=" + cancelled +
                ", diverted=" + diverted +
                ", whetherDelay=" + whetherDelay +
                '}';
    }
}

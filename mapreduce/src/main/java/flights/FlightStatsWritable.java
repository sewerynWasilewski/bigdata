package flights;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Składowe miar dla grupy (lotnisko wylotu, miesiąc wylotu).
 * Przechowywane są sumy i liczności (a nie gotowe średnie), dzięki czemu
 * możliwa jest agregacja wielopoziomowa (combiner -> reducer).
 */
public class FlightStatsWritable implements Writable {

    private long flightCount;   // liczba lotów
    private double priceSum;    // suma cen biletów (USD)
    private long delayedCount;  // liczba lotów ze statusem Delayed
    private double delaySum;    // suma opóźnień (min) lotów ze statusem Delayed

    public FlightStatsWritable() {
    }

    public void set(long flightCount, double priceSum, long delayedCount, double delaySum) {
        this.flightCount = flightCount;
        this.priceSum = priceSum;
        this.delayedCount = delayedCount;
        this.delaySum = delaySum;
    }

    public void add(FlightStatsWritable other) {
        this.flightCount += other.flightCount;
        this.priceSum += other.priceSum;
        this.delayedCount += other.delayedCount;
        this.delaySum += other.delaySum;
    }

    public long getFlightCount() {
        return flightCount;
    }

    public double getPriceSum() {
        return priceSum;
    }

    public long getDelayedCount() {
        return delayedCount;
    }

    public double getDelaySum() {
        return delaySum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(flightCount);
        out.writeDouble(priceSum);
        out.writeLong(delayedCount);
        out.writeDouble(delaySum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        flightCount = in.readLong();
        priceSum = in.readDouble();
        delayedCount = in.readLong();
        delaySum = in.readDouble();
    }

    @Override
    public String toString() {
        return flightCount + "," + priceSum + "," + delayedCount + "," + delaySum;
    }
}

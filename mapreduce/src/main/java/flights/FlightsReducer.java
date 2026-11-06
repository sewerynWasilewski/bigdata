package flights;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Sumuje składowe dla klucza "departure_airport_id,departure_month"
 * i wylicza miary końcowe.
 *
 * Wynik (separator ","):
 * departure_airport_id,departure_month,flight_count,avg_ticket_price,avg_delay_min
 */
public class FlightsReducer extends Reducer<Text, FlightStatsWritable, Text, Text> {

    private final FlightStatsWritable total = new FlightStatsWritable();
    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<FlightStatsWritable> values, Context context)
            throws IOException, InterruptedException {
        total.set(0, 0, 0, 0);
        for (FlightStatsWritable value : values) {
            total.add(value);
        }

        long flightCount = total.getFlightCount();
        double avgTicketPrice = total.getPriceSum() / flightCount;
        // brak lotów Delayed w grupie -> 0
        double avgDelayMin = total.getDelayedCount() == 0
                ? 0
                : total.getDelaySum() / total.getDelayedCount();

        outValue.set(flightCount + "," + avgTicketPrice + "," + avgDelayMin);
        context.write(key, outValue);
    }
}

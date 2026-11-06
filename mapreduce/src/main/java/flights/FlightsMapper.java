package flights;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Wejście: wiersz CSV z datasource1 (flights).
 * Wyjście: klucz "departure_airport_id,YYYY-MM", wartość FlightStatsWritable
 * ze składowymi miar dla pojedynczego lotu.
 *
 * Każdy rekord jest przetwarzany i emitowany od razu (bez buforowania).
 * Rekordy błędne są pomijane i zliczane w licznikach zadania.
 */
public class FlightsMapper extends Mapper<LongWritable, Text, Text, FlightStatsWritable> {

    public enum Records { VALID, HEADER, MALFORMED }

    private static final int FIELD_COUNT = 10;
    private static final int DEPARTURE_AIRPORT_ID = 2;
    private static final int SCHEDULED_DEPARTURE = 4;
    private static final int DELAY_MIN = 6;
    private static final int TICKET_PRICE_USD = 7;
    private static final int STATUS = 9;

    private static final String STATUS_DELAYED = "Delayed";
    // yyyy-MM-dd'T'HH:mm
    private static final Pattern DATE_TIME = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}");

    private final Text outKey = new Text();
    private final FlightStatsWritable outValue = new FlightStatsWritable();

    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String[] fields = line.toString().trim().split(",", -1);

        if ("flight_id".equals(fields[0])) {
            context.getCounter(Records.HEADER).increment(1);
            return;
        }
        if (fields.length != FIELD_COUNT) {
            context.getCounter(Records.MALFORMED).increment(1);
            return;
        }

        String airportId = fields[DEPARTURE_AIRPORT_ID].trim();
        String departure = fields[SCHEDULED_DEPARTURE].trim();
        String status = fields[STATUS].trim();
        if (airportId.isEmpty() || !DATE_TIME.matcher(departure).matches()) {
            context.getCounter(Records.MALFORMED).increment(1);
            return;
        }

        double price;
        long delayedCount = 0;
        double delay = 0;
        try {
            price = Double.parseDouble(fields[TICKET_PRICE_USD].trim());
            // opóźnienie uwzględniane wyłącznie dla lotów o statusie Delayed
            if (STATUS_DELAYED.equals(status)) {
                delay = Double.parseDouble(fields[DELAY_MIN].trim());
                delayedCount = 1;
            }
        } catch (NumberFormatException e) {
            context.getCounter(Records.MALFORMED).increment(1);
            return;
        }

        String departureMonth = departure.substring(0, 7); // YYYY-MM
        outKey.set(airportId + "," + departureMonth);
        outValue.set(1, price, delayedCount, delay);
        context.write(outKey, outValue);
        context.getCounter(Records.VALID).increment(1);
    }
}

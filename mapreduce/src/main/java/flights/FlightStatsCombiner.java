package flights;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Agregator łączący: sumuje składowe (liczności i sumy) dla klucza.
 * Format wyjścia identyczny z wyjściem mappera (Text, FlightStatsWritable),
 * więc combiner może być stosowany wielokrotnie.
 */
public class FlightStatsCombiner extends Reducer<Text, FlightStatsWritable, Text, FlightStatsWritable> {

    private final FlightStatsWritable result = new FlightStatsWritable();

    @Override
    protected void reduce(Text key, Iterable<FlightStatsWritable> values, Context context)
            throws IOException, InterruptedException {
        result.set(0, 0, 0, 0);
        for (FlightStatsWritable value : values) {
            result.add(value);
        }
        context.write(key, result);
    }
}

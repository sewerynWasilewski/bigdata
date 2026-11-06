package flights;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Zestaw 11 - flights-airports, MapReduce Classic.
 *
 * Użycie: hadoop jar flights-airports.jar flights.FlightsAirports input_dir1 output_dir3
 */
public class FlightsAirports extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: flights.FlightsAirports <input_dir1> <output_dir3>");
            return 2;
        }

        Configuration conf = getConf();
        // wynik w postaci CSV: klucz i wartość rozdzielone przecinkiem
        conf.set(TextOutputFormat.SEPARATOR, ",");

        Job job = Job.getInstance(conf, "flights-airports");
        job.setJarByClass(FlightsAirports.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(FlightsMapper.class);
        job.setCombinerClass(FlightStatsCombiner.class);
        job.setReducerClass(FlightsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlightStatsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new FlightsAirports(), args));
    }
}

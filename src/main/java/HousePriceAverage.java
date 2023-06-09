import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HousePriceAverage {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Coast,20538
            String sValue = value.toString();
            String[] values = sValue.split(",");
            context.write(new Text(values[0]),new Text(values[1] + ",1"));
        }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            long numberOfHOuses = 0L;
            double total = 0d;
            long houses = 0L;
            for (Text text : values) {
                String value = text.toString();
                String[] sValues = value.split(",");
                total += Double.parseDouble(sValues[0]);
                numberOfHOuses += Long.parseLong(sValues[1]);
            }
            context.write(key, new Text(total + "," + numberOfHOuses));
        }
    }
    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            long numberOfHOuses = 0L;
            double total = 0d;
            long houses = 0L;
            for (Text text : values) {
                String value = text.toString();
                String[] sValues = value.split(",");
                total += Double.parseDouble(sValues[0]);
                numberOfHOuses += Long.parseLong(sValues[1]);
            }
            context.write(key, new DoubleWritable(total / numberOfHOuses));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "exam scores average");
        job.setJarByClass(HousePriceAverage.class);
        // map step
        job.setMapperClass(MapClass.class);
        // combine step
//        job.setCombinerClass(Combine.class);
        //r reduce step
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

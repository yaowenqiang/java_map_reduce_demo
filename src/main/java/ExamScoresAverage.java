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

public class ExamScoresAverage {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            //1,Bigtown Academy,Physics,47
            String sValue = value.toString();
            String[] values = sValue.split(",");
            context.write(new Text(values[2]),new Text(values[3] + ",1"));
        }
    }


    public static class Combine extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {

            long numberOfCandidates = 0L;
            double totalScore = 0d;
            for (Text text : values) {
                String value = text.toString();
                String[] sValues = value.split(",");
                totalScore = totalScore + Double.parseDouble(sValues[0]);
                numberOfCandidates += Long.parseLong(sValues[1]);
            }

            context.write(key, new Text(totalScore + "," + numberOfCandidates));
        }
    }
    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            long numberOfCandidates = 0L;
            double totalScore = 0d;
            for (Text text : values) {
                String value = text.toString();
                String[] sValues = value.split(",");
                totalScore = totalScore + Double.parseDouble(sValues[0]);
                numberOfCandidates += Long.parseLong(sValues[1]);
            }
            double average = totalScore / numberOfCandidates;
            context.write(key, new DoubleWritable(average));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "exam scores average");
        job.setJarByClass(ExamScoresAverage.class);
        // map step
        job.setMapperClass(MapClass.class);
        // combine step
        job.setCombinerClass(Combine.class);
        //r reduce step
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

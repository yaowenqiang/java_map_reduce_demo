import java.io.IOException;
import java.util.StringTokenizer;

import jdk.nashorn.internal.ir.CallNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

    public static class ReduceClass
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "exam scores average");
        job.setJarByClass(ExamScoresAverage.class);
        // map step
        job.setMapperClass(TokenizerMapper.class);
        // combine step
        job.setCombinerClass(IntSumReducer.class);
        //r reduce step
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

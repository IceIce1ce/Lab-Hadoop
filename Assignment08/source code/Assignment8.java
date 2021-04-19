import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;


//Ex8
public class Assignment8{
    private enum COUNTERS {
        INVALID_RECORD_COUNT
    }

    public static final int user_id = 0, track_id = 1;

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable>{
        private IntWritable trackID = new IntWritable();
        private IntWritable userID = new IntWritable();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("[|]");
            trackID.set(Integer.parseInt(parts[Assignment8.track_id]));
            userID.set(Integer.parseInt(parts[Assignment8.user_id]));
            if(parts.length == 5) {
                context.write(trackID, userID);
            }
            else{
                context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            Set<Integer> userIDSet = new HashSet<>();
            for(IntWritable userID: values) {
                userIDSet.add(userID.get());
            }
            IntWritable size = new IntWritable(userIDSet.size());
            context.write(key, size);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "music track");
        job.setJarByClass(Assignment8.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
        System.out.println("No. of Invalid Records :" + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
    }
}
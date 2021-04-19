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


//Ex9
public class Assignment9{
    public static class TelecomRecord{
        public static final int fromPhone = 0, toPhone = 1, callStart = 2, callEnd = 3, flag = 4;
    }

    public static class Map extends Mapper<Object, Text, Text, LongWritable>{
        Text phoneNumber = new Text();
        LongWritable durationInMinutes = new LongWritable();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("[|]");
            if (parts[TelecomRecord.flag].equalsIgnoreCase("1")) {
                phoneNumber.set(parts[TelecomRecord.fromPhone]);
                String callStartTime = parts[TelecomRecord.callStart];
                String callEndTime = parts[TelecomRecord.callEnd];
                long duration = toMillis(callEndTime) - toMillis(callStartTime);
                durationInMinutes.set(duration / (1000 * 60));
                context.write(phoneNumber, durationInMinutes);
            }
        }

        private long toMillis(String date) {
            SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
            Date dateFrm = null;
            try {
                dateFrm = format.parse(date);
            }
            catch (ParseException e) {
                e.printStackTrace();
            }
            return Objects.requireNonNull(dateFrm).getTime();
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{
        LongWritable result = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable val: values){
                sum += val.get();
            }
            result.set(sum);
            if(sum >= 60){
                context.write(key, this.result);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "telecom record");
        job.setJarByClass(Assignment9.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
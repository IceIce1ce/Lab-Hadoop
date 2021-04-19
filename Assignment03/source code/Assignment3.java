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


//Ex3

public class Assignment3 {
    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable arg0, Text Value, Context context) throws IOException, InterruptedException {
            String line = Value.toString();
            try{
                if(!(line.length() == 0)){
                    String date = line.substring(6, 14); //14, 22
                    float temp_Max = Float.parseFloat(line.substring(39, 45).trim()); //104, 108
                    float temp_Min = Float.parseFloat(line.substring(47, 53).trim()); //112, 116
                    if (temp_Max > 40.0) {
                        context.write(new Text(date + "   Hot Day"), new Text(String.valueOf(temp_Max)));
                    }
                    if (temp_Min < 10.0) {
                        context.write(new Text(date + "   Cold Day"), new Text(String.valueOf(temp_Min)));
                    }
                }
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    public static class MaxTemperatureReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text Key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            String temperature = "";
            for(Text val: Values){
                temperature = val.toString();
            }
            context.write(Key, new Text("   " + temperature));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "weather temperature");
        job.setJarByClass(Assignment3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

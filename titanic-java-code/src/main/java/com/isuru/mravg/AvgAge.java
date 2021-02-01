package com.isuru.mravg;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AvgAge {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable age = new IntWritable();
        private Text gender = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            List<String> words = Arrays.asList(line.split(","));


            gender.set(words.get(4));
            if (words.get(5).length() == 0) {
                age.set(0);
            } else {
                age.set((int) Double.parseDouble(words.get(5)));
            }
            output.collect(gender, age);
           
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            int count = 0;

            while (values.hasNext()) {
                sum += values.next().get();
                count += 1;
            }

            output.collect(key, new IntWritable(sum / count));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(AvgAge.class);
        conf.setJobName("wordcount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}

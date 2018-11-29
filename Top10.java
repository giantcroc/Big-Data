import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Top10 {

    public static float total = 0;

    public static class TopMapper  extends Mapper<LongWritable, Text, Text, FloatWritable>{
        Text text =new Text();

        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String[] line= value.toString().split("\t");
            String keys = line[5];
            text.set(keys);
            context.write(text,new FloatWritable(1));
        }
    }


    public static class TopReducer extends Reducer< Text,FloatWritable, Text, FloatWritable>{
        Text text = new Text();
        TreeMap<Float,String > map = new TreeMap<Float,String>();
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> value, Context context)
                throws IOException, InterruptedException {
            float sum=0;
            for (FloatWritable ltext : value) {
                sum+=ltext.get();
            }
            map.put(sum,key.toString());
            total+=sum;
            if(map.size()>10){
                map.remove(map.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for(Float count:map.keySet()){
                context.write(new Text(map.get(count)), new FloatWritable(count/total));
            }
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "count");
        job.setJarByClass(Top10.class);
        job.setJobName("Top10");
	job.setJar("Top10.jar");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);



        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }




}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

import java.io.IOException;
import java.util.TreeMap;


public class Top30 {

    public static class TopMapper  extends Mapper<LongWritable, Text, Text, LongWritable>{
        Text text =new Text();

        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String[] line= value.toString().split("\t");
            String keys = line[2];
            text.set(keys);
            context.write(text,new LongWritable(1));
        }
    }


    public static class TopReducer extends Reducer< Text,LongWritable, Text, LongWritable>{
        Text text = new Text();
        TreeMap<Integer,String > map = new TreeMap<Integer,String>();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> value, Context context)
                throws IOException, InterruptedException {
            int sum=0;
            for (LongWritable ltext : value) {
                sum+=ltext.get();
            }
            map.put(sum,key.toString());

            if(map.size()>30){
                map.remove(map.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for(Integer count:map.keySet()){
                context.write(new Text(map.get(count)), new LongWritable(count));
            }
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "count");
        job.setJarByClass(Top30.class);
        job.setJobName("Top30");
	job.setJar("Top30.jar");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(TopMapper.class);
        job.setReducerClass(TopReducer.class);



        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }




}

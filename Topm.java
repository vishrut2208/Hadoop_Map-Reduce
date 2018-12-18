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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Topm {

    public static class Map extends Mapper<Object, Text, LongWritable, Text> {

        private LongWritable word = new LongWritable(); // type of output key

        public void map( Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split("\t");
            String[] mutualFriends = data[1].split(",");
                int k = mutualFriends.length;
                word.set(k);
                context.write(word, new Text(data[0] + "\t" + ":\t" + data[1] + "\n"));

        }
    }


    public static class Reduce
            extends Reducer<LongWritable,Text,Text,LongWritable> {

        private int mcount = 0;
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
              for(Text t : values) {
                  if (mcount < 10){
                      context.write(t, key);
              }
              mcount++;
              }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: <question_number> <input_files> <out>");
            System.exit(2);
        }
        {
            // create a job with name "mutual_friend"
            Job job = Job.getInstance(conf, "Top_10_mutual_friend");
            job.setJarByClass(Topm.class);

            // Setting Mapper and Reducer
            job.setNumReduceTasks(1);
            job.setMapperClass(Topm.Map.class);
            job.setReducerClass(Topm.Reduce.class);
            job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            // Setting Output format of Mapper and Input format of Reducer

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //set the HDFS path of the input data
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            if (!job.waitForCompletion(true))
                System.exit(1);
        }
    }
}


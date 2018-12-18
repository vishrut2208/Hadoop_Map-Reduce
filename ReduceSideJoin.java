import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceSideJoin {


    public static class Map extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text(); // type of output key

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split("\t");
            if (data.length == 2) {
                String[] friends = data[1].split(",");

                for (String i : friends) {

                    if (i.trim().length() > 0) {

                        word.set(data[0]);
                        context.write(word, new Text(i));
                    }
                }
            }
        }
    }


    public static class idMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object Key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            String record = value.toString();
            String[] columns = record.split(",");

            int age = 2018 - (Integer.parseInt(columns[9].split("/")[2]));

            context.write(new Text(columns[0]), new Text("age:\t" + age + "\t" + "Address:\t" + columns[3] + ", " + columns[4] + ", " + columns[5]));

        }
    }


    public static class minReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> userfriend = new ArrayList<>();
            String age = new String();

            for (Text t : values) {
                String parts[] = t.toString().split("\t");

                if (parts[0].equals("age:") && (parts.length > 3)) {
                     age = parts[1];
                }else{

                userfriend.add(parts[0]);
            }}

            for (int i = 0; i < userfriend.size(); i++) {

                context.write(new Text(userfriend.get(i)), new Text( key + "\t" + age));
            }
        }

    }



    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        {
            Job job1 = Job.getInstance(conf, "Reduce_side_join_AGE_MAPPING");
            job1.setJarByClass(ReduceSideJoin.class);
            MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, ReduceSideJoin.Map.class);
            MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, ReduceSideJoin.idMapper.class);
            job1.setReducerClass(ReduceSideJoin.minReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job1, new Path(args[2] + "/temp1"));
            if (!job1.waitForCompletion(true))
                System.exit(1);
        }


        {
            Job job2 = Job.getInstance(conf, "Friend_MIN_AGE_Address_Mapping");
            job2.setJarByClass(MinAge.class);
            MultipleInputs.addInputPath(job2, new Path(args[2] + "/temp1/part-r-00000"), TextInputFormat.class, MinAge.minMap.class);
            MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, MinAge.minidMapper.class);
            job2.setReducerClass(MinAge.Reduce.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/temp2"));
            if (!job2.waitForCompletion(true))
                System.exit(1);

        }


        {
            Job job3 = Job.getInstance(conf, "orderage_top10");
            job3.setJarByClass(DecOrder.class);

            // Setting Mapper and Reducer
            job3.setNumReduceTasks(1);
            job3.setMapperClass(DecOrder.MapDec.class);
            job3.setReducerClass(DecOrder.ReduceDec.class);
            job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            // Setting Output format of Mapper and Input format of Reducer
            job3.setMapOutputKeyClass(LongWritable.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(LongWritable.class);

            //set the HDFS path of the input data
            FileInputFormat.addInputPath(job3, new Path(args[2] + "/temp2"));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job3, new Path(args[2] + "/temp3"));

            if (!job3.waitForCompletion(true))
                System.exit(1);
        }

    }
}




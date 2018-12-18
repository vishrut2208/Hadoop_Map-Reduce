import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;

public class MutualFriend {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text(); // type of output key

        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split("\t");
            if (data.length == 2) {
                String[] friends = data[1].split(",");
                String pass = new String();

                for (String i : friends) {
                    if (i.trim().length() > 0) {
                        if ((Integer.parseInt(data[0])) < (Integer.parseInt(i))) {
                            pass = data[0] + "," + i;
                        } else {
                            pass = i + "," + data[0];
                        }
                        word.set(pass);
                        context.write(word, new Text(data[1]));
                    }
                }
            }

        }
    }


    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {

        private Text result = new Text();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String []> store = new ArrayList<>();

            for(Text t : values){
                store.add(t.toString().split(","));
            }

            StringBuilder res = new StringBuilder();
            if(store.size() == 2){
               for(int i=0 ; i < store.get(0).length ; i++){
                   for(int j=0 ; j< store.get(1).length ; j++){
                       if(store.get(0)[i].equals(store.get(1)[j])){
                        res.append(store.get(0)[i] + ",");
                        }
                   }
               }
                //res.deleteCharAt(res.length()-1);
                String resu = res.toString();

            if(resu.length()>0){
            result.set(resu);
            context.write(key, result);
            }
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
            Job job = Job.getInstance(conf, "mutual_friend");
            job.setJarByClass(MutualFriend.class);

            // Setting Mapper and Reducer
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //set the HDFS path of the input data
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            if (!job.waitForCompletion(true))
                System.exit(1);
        }
    }
}


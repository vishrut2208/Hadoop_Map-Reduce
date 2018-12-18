import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;


public class MFAdd {

    public static class Map extends Mapper<Object, Text, Text, Text>{

        HashMap<String,String> map = new HashMap<>();
        Text user = new Text();

        public void map( Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split("\t");
           // if(data.length == 2) {
                String[] mutualFriends = data[1].split(",");

                if (new Text(data[0]).equals(user)) {

                    StringBuilder sb = new StringBuilder();
                    sb.append("[");
                    for (String f : mutualFriends) {
                        if(f.trim().length()>0){
                            if(f != null) {

                            String[] res = map.get(f.trim()).toString().split(":");
                                if(res.length >2){
                                sb.append(res[1] + ":" + res[2]);
                                sb.append(", ");
                                }
                            }
                        }
                    }
                    if(sb.toString().length() > 2){
                    sb.deleteCharAt(sb.length()-1);
                    sb.deleteCharAt(sb.length()-1);}
                    sb.append("]");
                    context.write(new Text(data[0]), new Text(sb.toString()));
                }

            //}
        }

        protected void setup(Mapper.Context context)
                throws IOException, InterruptedException {

            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path part = new Path(context.getConfiguration().get("USER_AGENT_PATH"));//Location of file in HDFS
            Text user1 = new Text(context.getConfiguration().get("USER_1"));
            Text user2 = new Text(context.getConfiguration().get("USER_2"));

            if (user1.compareTo(user2) > 0) {
                Text temp = user2;
                user2 = user1;
                user1 = temp;
            }
            user = new Text(user1 + "," + user2);

            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fss = fs.listStatus(part);
            for (FileStatus status : fss) {
                Path pt = status.getPath();

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] arr = line.split(",");
                    map.put(arr[0].trim(), arr[0] + ":" + arr[1] + ":" + arr[5]);
                    line = br.readLine();
                }
            }
        }

    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length != 5) {
            System.err.println("Usage: <question_number> <input_files_adj_list> <input_files_user_data> <output_folder> <user1_id> <user2_id>");
            System.exit(2);
        }
        // Job 3 -- Calculate mutual friends detail for a given pair of user
        {
            conf.set("USER_AGENT_PATH",otherArgs[1]);
            conf.set("USER_1", otherArgs[3]);
            conf.set("USER_2", otherArgs[4]);

            // create a job with name "mutual_friend_for_a_pair"
            Job job3 = Job.getInstance(conf, "mutual_friend_for_a_pair");
            job3.setJarByClass(MFAdd.class);
            job3.setMapperClass(MFAdd.Map.class);
            //job3.setInputFormatClass(KeyValueTextInputFormat.class);
            // job3.setReducerClass(MfAddress.Reduce.class);

            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job3, new Path(otherArgs[0]));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job3, new Path(otherArgs[2]));
            System.exit(job3.waitForCompletion(true) ? 0 : 1);
        }
    }
}

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DecOrder {

    public static class MapDec extends Mapper<Object, Text, LongWritable, Text>{

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException{

            String[] parts = value.toString().split("\t");

            if(parts.length > 4) {
                int age = Integer.parseInt(parts[4]);

                context.write(new LongWritable(age), new Text("User:\t" + parts[0] + "\t" + "UserName:\t" + parts[1] + "\tAddress:\t" + parts[2] + "\t" + "Friend:\t" + parts[3] + "\t" +"Friend's Age:"));
            }
        }
    }



    public static class ReduceDec extends Reducer<LongWritable, Text, Text, LongWritable>{
        private int mcount = 0;
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            for (Text i : values){
                if(mcount < 10){
                context.write(i, key);
                }
                mcount++;
            }
        }
    }

}


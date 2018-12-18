import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinAge {

    public static class minMap extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException{

            String[] parts = value.toString().split("\t");
            //if(parts.length > 0) {
            context.write(new Text(parts[0]), new Text(parts[1] + "\t" + parts[2]));
            //}
        }
    }

    public static class minidMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object Key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            String record = value.toString();
            String[] columns = record.split(",");

            int age = 2018 - (Integer.parseInt(columns[9].split("/")[2]));

            context.write(new Text(columns[0]), new Text( "Name:\t"+ columns[1] + " " + columns[2] +"\t" +"Address:\t" + columns[3] + ", " + columns[4] + ", " + columns[5]));

        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String result = new String();
            int com = 100;
            String address = new String();

            for (Text i : values) {
                String parts[] = i.toString().split("\t");


                if (parts[0].equals("Name:")) {
                    address = parts[1] + "\t" + parts[3];
                } else if (parts.length > 0) {
                    if (Integer.parseInt(parts[1].trim()) < com) {
                        com = Integer.parseInt(parts[1].trim());
                        result = parts[0] + "\t" + parts[1];
                    }
                }
            }

            result = address + "\t" + result;
            context.write(key, new Text(result));
        }
    }
}
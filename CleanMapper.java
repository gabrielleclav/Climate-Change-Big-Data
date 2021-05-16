import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

      String data = value.toString();
//      if (data.startsWith("population")){
//        return;
//      }
      //split data
      String[] split = data.split(',');
      String country = split[0];
      String year = split[2];
      String population = split[3];
      if (Integer.parseInt(year) > 1990){
        context.write(new Text(country+ ","+year+','+population), new IntWritable(1));
      }
  }
}

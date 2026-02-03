import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataProfilingMapper 
    extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final String[] HEADER = new String[] {
        "ride_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "start_station_name",
        "start_station_id",
        "end_station_name",
        "end_station_id",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_casual"
    };

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("ride_id")) return;

        String[] features = line.split(",", -1);

        for (int i = 0; i < HEADER.length; i++) {
            String columnName = HEADER[i];
            String data = features[i];
            context.write(new Text(columnName), new Text(data));
        }

    }
}


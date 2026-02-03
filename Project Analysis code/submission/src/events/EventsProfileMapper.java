package events;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class EventsProfileMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString();
        
        if (key.get() == 0 && line.toLowerCase().contains("event id")) {
            return;
        }
        
        if (line.trim().isEmpty()) {
            return;
        }
        
        String[] fields = line.split(",");
        
        context.write(new Text("row_count"), new Text("1"));
        
        if (fields.length != 12) {
            context.write(new Text("malformed_row"), new Text("1"));
            return;
        }
        
        String eventId = fields[0].trim();
        String eventName = fields[1].trim();
        String startTime = fields[2].trim();
        String endTime = fields[3].trim();
        String eventType = fields[5].trim();
        String eventLocation = fields[7].trim();
        
        if (eventId.isEmpty()) {
            context.write(new Text("empty_event_id"), new Text("1"));
        }
        
        if (eventName.isEmpty()) {
            context.write(new Text("empty_event_name"), new Text("1"));
        }
        
        if (startTime.isEmpty()) {
            context.write(new Text("empty_start_time"), new Text("1"));
        } else {
            try {
                dateFormat.parse(startTime);
            } catch (Exception e) {
                context.write(new Text("invalid_timestamp_start"), new Text("1"));
            }
        }
        
        if (endTime.isEmpty()) {
            context.write(new Text("empty_end_time"), new Text("1"));
        } else {
            try {
                dateFormat.parse(endTime);
            } catch (Exception e) {
                context.write(new Text("invalid_timestamp_end"), new Text("1"));
            }
        }
        
        if (eventType.isEmpty()) {
            context.write(new Text("empty_event_type"), new Text("1"));
        } else {
            context.write(new Text("event_type:" + eventType), new Text("1"));
        }
        
        if (eventLocation.isEmpty()) {
            context.write(new Text("empty_event_location"), new Text("1"));
            context.write(new Text("location_mapping_missing"), new Text("1"));
        } else {
            context.write(new Text("event_location:" + eventLocation), new Text("1"));
        }
        
        if (!startTime.isEmpty()) {
            try {
                dateFormat.parse(startTime);
                context.write(new Text("min_start_time"), new Text(startTime));
                context.write(new Text("max_start_time"), new Text(startTime));
            } catch (Exception e) {
            }
        }
        
        if (!endTime.isEmpty()) {
            try {
                dateFormat.parse(endTime);
                context.write(new Text("min_end_time"), new Text(endTime));
                context.write(new Text("max_end_time"), new Text(endTime));
            } catch (Exception e) {
            }
        }
    }
}

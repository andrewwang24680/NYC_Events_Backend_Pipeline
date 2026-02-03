package events;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EventsProfileReducer extends Reducer<Text, Text, Text, Text> {
    
    private SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
    private SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        String keyStr = key.toString();
        
        if (keyStr.startsWith("event_type:") || keyStr.startsWith("event_location:") ||
            keyStr.equals("row_count") || keyStr.equals("invalid_timestamp_start") ||
            keyStr.equals("invalid_timestamp_end") || keyStr.equals("malformed_row") ||
            keyStr.equals("empty_event_id") || keyStr.equals("empty_event_name") ||
            keyStr.equals("empty_start_time") || keyStr.equals("empty_end_time") ||
            keyStr.equals("empty_event_type") || keyStr.equals("empty_event_location") ||
            keyStr.equals("location_mapping_missing")) {
            
            long count = 0;
            for (Text value : values) {
                count += Long.parseLong(value.toString());
            }
            context.write(key, new Text(String.valueOf(count)));
        }
        else if (keyStr.equals("min_start_time") || keyStr.equals("min_end_time")) {
            Date minDate = null;
            for (Text value : values) {
                try {
                    Date date = inputFormat.parse(value.toString());
                    if (minDate == null || date.before(minDate)) {
                        minDate = date;
                    }
                } catch (Exception e) {
                }
            }
            if (minDate != null) {
                context.write(key, new Text(outputFormat.format(minDate)));
            }
        }
        else if (keyStr.equals("max_start_time") || keyStr.equals("max_end_time")) {
            Date maxDate = null;
            for (Text value : values) {
                try {
                    Date date = inputFormat.parse(value.toString());
                    if (maxDate == null || date.after(maxDate)) {
                        maxDate = date;
                    }
                } catch (Exception e) {
                }
            }
            if (maxDate != null) {
                context.write(key, new Text(outputFormat.format(maxDate)));
            }
        }
    }
}

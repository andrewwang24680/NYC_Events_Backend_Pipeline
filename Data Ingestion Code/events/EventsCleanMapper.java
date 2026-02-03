package events;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class EventsCleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    
    private SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
    private SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private HashMap<String, String[]> locationMap = new HashMap<>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String lookupPath = conf.get("locations.lookup");
        
        if (lookupPath == null || lookupPath.isEmpty()) {
            throw new IOException("Need location lookup file: -Dlocations.lookup=<path>");
        }
        
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(lookupPath))));
        
        String line = reader.readLine();
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length >= 4) {
                String rawLocation = fields[0].trim().replaceAll("^\"|\"$", "");
                String lat = fields[1].trim().replaceAll("^\"|\"$", "");
                String lon = fields[2].trim().replaceAll("^\"|\"$", "");
                String locId = fields[3].trim().replaceAll("^\"|\"$", "");
                
                if (!rawLocation.isEmpty() && !lat.isEmpty() && !lon.isEmpty()) {
                    locationMap.put(rawLocation, new String[] {lat, lon, locId});
                }
            }
        }
        reader.close();
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString();
        
        if (key.get() == 0 && line.toLowerCase().contains("event id")) {
            return;        }
        
        if (line.trim().isEmpty()) {
            return;
        }
        
        String[] fields = line.split(",");
        
        if (fields.length != 12) {
            return;
        }
        
        String eventId = fields[0].trim().replaceAll("^\"|\"$", "");
        String eventName = fields[1].trim().replaceAll("^\"|\"$", "");
        String startTime = fields[2].trim().replaceAll("^\"|\"$", "");
        String endTime = fields[3].trim().replaceAll("^\"|\"$", "");
        String eventType = fields[5].trim().replaceAll("^\"|\"$", "");
        String eventLocation = fields[7].trim().replaceAll("^\"|\"$", "");
        
        if (eventId.isEmpty() || eventName.isEmpty() || startTime.isEmpty() || endTime.isEmpty()) {
            return;
        }
        
        String startTs = null;
        String endTs = null;
        try {
            Date startDate = inputFormat.parse(startTime);
            Date endDate = inputFormat.parse(endTime);
            startTs = outputFormat.format(startDate);
            endTs = outputFormat.format(endDate);
            
            if (startDate.after(endDate)) {
                return;
            }
        } catch (Exception e) {
            return;
        }
        
        String normEventName = eventName.trim().replaceAll("\\s+", " ");
        
        if (eventType.trim().isEmpty()) {
            return;
        }
        
        String[] locInfo = locationMap.get(eventLocation);
        String lat, lon, locId;
        if (locInfo != null) {
            lat = locInfo[0];
            lon = locInfo[1];
            locId = locInfo[2];
        } else {
            lat = "0.0";
            lon = "0.0";
            locId = "UNKNOWN";
        }
        
        String output = eventId + "," + normEventName + "," + startTs + "," + endTs + 
                       "," + eventType.trim() + "," + lat + "," + lon + "," + locId;
        
        context.write(NullWritable.get(), new Text(output));
    }
}

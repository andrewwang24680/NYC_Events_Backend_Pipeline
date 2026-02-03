import java.io.IOException;

import javax.naming.Context;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DataCleaningMapper 
    extends Mapper<LongWritable, Text, NullWritable, Text> {

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

    private static final String BEGIN_TIME = "2024-01-01 00:00:00.000";
    private static final String END_TIME   = "2025-11-01 00:00:00.000";
    private static final double MIN_LAT = 40.49;
    private static final double MAX_LAT = 40.92;
    private static final double MIN_LNG = -74.26;
    private static final double MAX_LNG = -73.68;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("ride_id")) return;

        String[] features = line.split(",", -1);
        if (features.length < HEADER.length) return;

        String rideId          = features[0].trim();
        String rideableType    = features[1].trim();
        String startedAt       = features[2].trim();
        String endedAt         = features[3].trim();
        String startStation    = features[4].trim();
        String startStationId  = features[5].trim();
        String endStation      = features[6].trim();
        String endStationId    = features[7].trim();
        String startLat        = features[8].trim();
        String startLng        = features[9].trim();
        String endLat          = features[10].trim();
        String endLng          = features[11].trim();
        String memberCasual    = features[12].trim();

        if (rideId.isEmpty() || rideableType.isEmpty() || memberCasual.isEmpty()) return;

        if (!isValidTimeInRange(startedAt) || !isValidTimeInRange(endedAt) || endedAt.compareTo(startedAt) <= 0) return;

        if (!isValidLat(startLat) || !isValidLat(endLat) || !isValidLng(startLng) || !isValidLng(endLng)) return;

        if (startStation.isEmpty()|| endStation.isEmpty()) return;

        if (!isValidStationID(startStationId) || !isValidStationID(endStationId)) return;

        context.write(NullWritable.get(), value);

    }

    public boolean isValidTimeInRange(String s) {
        String time_s = s.trim();
        if (time_s.isEmpty()) {
            return false;
        }
        String dateString = time_s;

        if (dateString.length() == 19) {
            dateString = dateString + ".000";
        } else if (dateString.length() != 23) {
            return false;
        }

        if (dateString.compareTo(BEGIN_TIME) < 0) return false;
        if (dateString.compareTo(END_TIME)   > 0) return false;

        return true;
    }

    public boolean isValidLat(String s) {
        String trimmed = s.trim();
        if (trimmed.isEmpty()) return false;
        try {
            double v = Double.parseDouble(trimmed);
            if (!Double.isFinite(v)) return false;
            return (v >= MIN_LAT && v <= MAX_LAT);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean isValidLng(String s) {
        String trimmed = s.trim();
        if (trimmed.isEmpty()) return false;
        try {
            double v = Double.parseDouble(trimmed);
            if (!Double.isFinite(v)) return false;
            return (v >= MIN_LNG && v <= MAX_LNG);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean isValidStationID(String s) {
        String trimmed = s.trim();
        if (trimmed.isEmpty()) return false;
        return trimmed.matches("\\d+(\\.\\d+)?");
    }
}   


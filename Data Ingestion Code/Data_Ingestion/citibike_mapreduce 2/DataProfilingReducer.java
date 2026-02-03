import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;

import javax.naming.Context;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataProfilingReducer 
    extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            
        String columnName = key.toString();
        
        if (columnName.equals("ride_id")) {
            ProfileRideId(columnName, values, context);
        }
        else if (columnName.equals("rideable_type") || columnName.equals("member_casual")) {
            profileSmallType(columnName, values, context);
        }
        else if (columnName.equals("start_station_name") || columnName.equals("end_station_name")) {
            profileStation(columnName, values, context);
        }
        else if (columnName.equals("start_station_id") || columnName.equals("end_station_id") ){
            profileStationByID(columnName, values, context);
        }
        else if (columnName.equals("started_at") || columnName.equals("ended_at")) {
            profileTimeRange(columnName, values, context);
        }
        else if (columnName.equals("start_lat") || columnName.equals("start_lng")
            || columnName.equals("end_lat") || columnName.equals("end_lng")) {
            profileLatAndLng(columnName, values, context);
        }
        
    }

    public void ProfileRideId (String name, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
        
        long totalCount = 0L;
        long missingCount = 0L;
        // HashSet<String> uniqueIds = new HashSet<>();
        // long duplicateCount = 0L;

        for (Text t: values) {
            totalCount++;
            String s = t.toString().trim();
            if (s.isEmpty()) {
                missingCount++;
            }
            // else {
            //     if (!uniqueIds.add(s)) {
            //         duplicateCount++;
            //     }
            // }
        }

        double missPer = totalCount > 0 ? (missingCount * 100.0 / totalCount) : 0.0;
        // double duplicatePer = totalCount > 0 ? (duplicateCount * 100.0 / totalCount) : 0.0;
        // double uniquePer = totalCount > 0 ? (uniqueIds.size() * 100.0 / totalCount) : 0.0;


        // String summary = String.format("Total=%d, missing=%d(%.2f%%), duplicates=%d(%.2f%%), unique=%d(%.2f%%)", 
        //     totalCount, missingCount, missPer, duplicateCount, duplicatePer, uniqueIds.size(), uniquePer);
        
        String summary = String.format("Total=%d, missing=%d(%.2f%%)", totalCount, missingCount, missPer);
            
        context.write(new Text(name), new Text(summary));

    }

    public void profileSmallType (String name, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {

        long totalCount = 0L;
        long missingCount = 0L;

        HashMap<String, Long> counter = new HashMap<>();

        for (Text t: values) {
            totalCount++;
            String s = t.toString().trim();
            if (s.isEmpty()) {
                missingCount++;
            }
            else {
                Long times = counter.getOrDefault(s, 0L);
                counter.put(s, times + 1);
            }
        }

        double missPer = totalCount > 0 ? (missingCount * 100.0 / totalCount) : 0.0;

        String distribution = counter.entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(", ", "{", "}"));

        String summary = String.format(
            "Total=%d, missing=%d(%.2f%%), unique=%d, values=%s",
            totalCount, missingCount, missPer, counter.size(), distribution);

        context.write(new Text(name), new Text(summary));

    }

    public void profileStationByID (String name, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
    
        long totalCount = 0L;
        long missingCount = 0L;
        long invalidFormatCount = 0L;
        // HashSet<String> uniqueStation = new HashSet<>();

        // Ex: 8465.01 

        for (Text t: values) {
            totalCount++;
            String s = t.toString().trim();
            if (s.isEmpty()) {
                missingCount++;
            }
            else {
                 // uniqueStation.add(s);
                if (!s.matches("\\d+(\\.\\d+)?")) {
                    invalidFormatCount++;
                }
            }
        }

        double missPer = totalCount > 0 ? (missingCount * 100.0 / totalCount) : 0.0;
        double invalidPer = totalCount > 0 ? (invalidFormatCount * 100.0 / totalCount) : 0.0;
        // double uniquePer = totalCount > 0 ? (uniqueStation.size() * 100.0 / totalCount) : 0.0;

        // String summary = String.format(
        //     "Total=%d, missing=%d(%.2f%%), unique=%d(%.2f%%), invalid_format=%d(%.2f%%)",
        //     totalCount, missingCount, missPer, uniqueStation.size(), uniquePer, invalidFormatCount, invalidPer);

        String summary = String.format(
            "Total=%d, missing=%d(%.2f%%), invalid_format=%d(%.2f%%)",
            totalCount, missingCount, missPer, invalidFormatCount, invalidPer);
        
        context.write(new Text(name), new Text(summary));

    }

    public void profileStation (String name, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
    
        long totalCount = 0L;
        long missingCount = 0L;

        // HashSet<String> uniqueStation = new HashSet<>();

        for (Text t: values) {
            totalCount++;
            String s = t.toString().trim();
            if (s.isEmpty()) {
                missingCount++;
            }
            // else {
            //     uniqueStation.add(s);
            // }
        }

        double missPer = totalCount > 0 ? (missingCount * 100.0 / totalCount) : 0.0;
        // double uniquePer = totalCount > 0 ? (uniqueStation.size() * 100.0 / totalCount) : 0.0;

        // String summary = String.format(
        //     "Total=%d, missing=%d(%.2f%%), unique=%d(%.2f%%)",
        //     totalCount, missingCount, missPer, uniqueStation.size(), uniquePer);
        
         String summary = String.format("Total=%d, missing=%d(%.2f%%)", totalCount, missingCount, missPer);
        
        context.write(new Text(name), new Text(summary));

    }

    public void profileTimeRange (String name, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {

        long totalCount = 0L;
        long missingCount = 0L;
        long invalidCount = 0L;
        long outOfRangeCount = 0L;
        long missPrecision = 0L;

        // Ex: 2025-05-16 06:26:05.309
        
        String minimumTime = null;
        String maximumTime = null;
        String beginTime = "2024-01-01 00:00:00.000";
        String endTime = "2025-11-01 00:00:00.000";

        for (Text t : values) {
            totalCount++;
            String s = t.toString().trim();

            if (s.isEmpty()) {
                missingCount++;
            }
            else {
                String dateString = s;
                if (s.length() == 19) {
                    dateString = s + ".000";
                    missPrecision++;
                }
                else if (s.length() != 23) {
                    invalidCount++;
                    continue;
                }

                if (minimumTime == null || dateString.compareTo(minimumTime) < 0) {
                    minimumTime = dateString;
                }

                if (maximumTime == null || dateString.compareTo(maximumTime) > 0) {
                    maximumTime = dateString;
                }

                if (dateString.compareTo(beginTime) < 0 || dateString.compareTo(endTime) > 0) {
                    outOfRangeCount++;
                }
            }
        }

        double missPer = totalCount > 0 ? (missingCount * 100.0 / totalCount) : 0.0;
        double invalidPer = totalCount > 0 ? (invalidCount * 100.0 / totalCount) : 0.0;
        double missPrecisionPer = totalCount > 0 ? (missPrecision * 100.0 / totalCount) : 0.0;
        double outOfRangeCountPer = totalCount > 0 ? (outOfRangeCount * 100.0 / totalCount) : 0.0;

        String summary = String.format(
            "Total=%d, missing=%d(%.2f%%), invalid=%d(%.2f%%), miss_precision=%d(%.2f%%), out_of_range=%d(%.2f%%),  min_time=%s, max_time=%s, range=[%s to %s]",
                totalCount, missingCount, missPer, invalidCount, invalidPer, missPrecision, missPrecisionPer, 
                outOfRangeCount, outOfRangeCountPer, minimumTime != null ? minimumTime : "N/A",
                maximumTime != null ? maximumTime : "N/A", beginTime, endTime );
        
        context.write(new Text(name), new Text(summary));

    }

    public void profileLatAndLng (String name, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
        
        long totalCount = 0L;
        long missingCount = 0L;
        long invalidFormatCount = 0L;
        long outOfRangeCount = 0L;

        final double MIN_LAT = 40.49;
        final double MAX_LAT = 40.92;
        final double MIN_LNG = -74.26;
        final double MAX_LNG = -73.68;

        boolean isLat = name.contains("lat"); // start_lat or end_lat
        boolean isLng = name.contains("lng"); // start_lng or end_lng

        for (Text t: values) {
            totalCount++;
            String s = t.toString().trim();
            if (s.isEmpty()) {
                missingCount++;
            }
            else {
                try {
                    double location = Double.parseDouble(s);
                    
                    if (!Double.isFinite(location)) {
                        invalidFormatCount++;
                        continue;
                    }

                    if (isLat) {
                        if (location < MIN_LAT || location > MAX_LAT) {
                            outOfRangeCount++;
                        } 
                    }
                    else if (isLng) {
                        if (location < MIN_LNG || location > MAX_LNG) {
                            outOfRangeCount++;
                        }
                    }
                } catch (NumberFormatException e) {
                    invalidFormatCount++;
                }
            }
        }

        double missPer = totalCount > 0 ? (missingCount * 100.0 / totalCount) : 0.0;
        double invalidPer = totalCount > 0 ? (invalidFormatCount * 100.0 / totalCount) : 0.0;
        double outOfRangePer = totalCount > 0 ? (outOfRangeCount * 100.0 / totalCount) : 0.0;

        String summary = String.format("Total=%d, missing=%d(%.2f%%), invalid=%d(%.2f%%), outOfRange=%d(%.2f%%)", 
        totalCount, missingCount, missPer, invalidFormatCount, invalidPer, outOfRangeCount, outOfRangePer);
        context.write(new Text(name), new Text(summary));

    }
}


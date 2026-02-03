package events;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class EventsCleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    
    private SimpleDateFormat[] inputFormats = {
        new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a"),
        new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
        new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"),
        new SimpleDateFormat("MM-dd-yyyy hh:mm:ss a"),
        new SimpleDateFormat("MM-dd-yyyy HH:mm:ss"),
        new SimpleDateFormat("M/d/yyyy hh:mm:ss a"),
        new SimpleDateFormat("M/d/yyyy HH:mm:ss"),
        new SimpleDateFormat("MM/dd/yyyy"),
        new SimpleDateFormat("yyyy-MM-dd"),
        new SimpleDateFormat("M/d/yyyy")
    };
    private SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Map<String, String> zoneLookup = new HashMap<>();
    
    @Override
    public void setup(Context context) throws IOException {
        Path[] cacheFiles = context.getLocalCacheFiles();
        if (cacheFiles == null || cacheFiles.length == 0) {
            return;
        }
        
        BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
        String line;
        reader.readLine();
        
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) continue;
            
            String[] fields = parseCSVLine(line);
            if (fields.length >= 5) {
                String zoneName = fields[3].trim();
                String locationId = fields[4].trim();
                
                if (!zoneName.isEmpty() && !locationId.isEmpty()) {
                    try {
                        Integer.parseInt(locationId);
                        if (!zoneName.toLowerCase().contains("special event") && 
                            !locationId.toLowerCase().contains("special event")) {
                            String standardizedZone = standardizeLocationName(zoneName);
                            zoneLookup.put(standardizedZone, locationId);
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }
        reader.close();
    }
    
    private String standardizeLocationName(String name) {
        if (name == null || name.isEmpty()) {
            return "";
        }
        
        String standardized = name.toUpperCase();
        standardized = standardized.replaceAll("[:,\\.\\-/]", " ");
        standardized = standardized.replaceAll("\\s*-?\\s*\\d+\\s*$", "");
        standardized = standardized.replaceAll("\\s+", " ");
        standardized = standardized.trim();
        
        return standardized;
    }
    
    private String findLocationBySubstring(String standardizedEventLocation) {
        if (standardizedEventLocation == null || standardizedEventLocation.isEmpty()) {
            return null;
        }
        
        String bestMatch = null;
        int bestMatchLength = 0;
        
        for (Map.Entry<String, String> entry : zoneLookup.entrySet()) {
            String standardizedZone = entry.getKey();
            String locationId = entry.getValue();
            
            if (standardizedZone.isEmpty()) {
                continue;
            }
            
            try {
                Integer.parseInt(locationId);
            } catch (NumberFormatException e) {
                continue;
            }
            
            if (standardizedEventLocation.contains(standardizedZone) || 
                standardizedZone.contains(standardizedEventLocation)) {
                int matchLength = Math.min(standardizedEventLocation.length(), standardizedZone.length());
                if (matchLength > bestMatchLength) {
                    bestMatch = locationId;
                    bestMatchLength = matchLength;
                }
            }
        }
        
        return bestMatch;
    }
    
    private String formatTimestamp(String timestamp) {
        if (timestamp == null || timestamp.trim().isEmpty()) {
            return null;
        }
        
        String trimmed = timestamp.trim().replaceAll("^\"|\"$", "");
        
        for (SimpleDateFormat format : inputFormats) {
            try {
                format.setLenient(false);
                Date date = format.parse(trimmed);
                return outputFormat.format(date);
            } catch (Exception e) {
                continue;
            }
        }
        
        try {
            String cleaned = trimmed.replaceAll("[^0-9/\\-:\\s]", " ");
            String[] parts = cleaned.trim().split("[\\s/\\-:]+");
            if (parts.length >= 3) {
                String year, month, day;
                if (parts[0].length() == 4) {
                    year = parts[0];
                    month = String.format("%02d", Integer.parseInt(parts[1]));
                    day = String.format("%02d", Integer.parseInt(parts[2]));
                } else {
                    year = parts[2].length() == 4 ? parts[2] : "20" + parts[2];
                    month = String.format("%02d", Integer.parseInt(parts[0]));
                    day = String.format("%02d", Integer.parseInt(parts[1]));
                }
                String datePart = year + "-" + month + "-" + day;
                if (parts.length >= 6) {
                    return String.format("%s %02d:%02d:%02d", datePart, 
                        Integer.parseInt(parts[3]), Integer.parseInt(parts[4]), Integer.parseInt(parts[5]));
                } else {
                    return datePart + " 00:00:00";
                }
            }
        } catch (Exception e) {
        }
        
        return null;
    }
    
    private String[] parseCSVLine(String line) {
        java.util.List<String> fields = new java.util.ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentField = new StringBuilder();
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(currentField.toString());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }
        fields.add(currentField.toString());
        return fields.toArray(new String[0]);
    }
    
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
        
        String[] fields = parseCSVLine(line);
        
        if (fields.length < 4) {
            return;
        }
        
        String eventId = fields.length > 0 ? fields[0].trim().replaceAll("^\"|\"$", "") : "";
        String eventName = fields.length > 1 ? fields[1].trim().replaceAll("^\"|\"$", "") : "";
        String startTime = fields.length > 2 ? fields[2].trim().replaceAll("^\"|\"$", "") : "";
        String endTime = fields.length > 3 ? fields[3].trim().replaceAll("^\"|\"$", "") : "";
        String eventType = fields.length > 5 ? fields[5].trim().replaceAll("^\"|\"$", "") : "";
        String eventLocation = fields.length > 7 ? fields[7].trim().replaceAll("^\"|\"$", "") : "";
        
        if (eventId.isEmpty() || eventName.isEmpty() || startTime.isEmpty() || endTime.isEmpty()) {
            return;
        }
        
        String startTs = formatTimestamp(startTime);
        String endTs = formatTimestamp(endTime);
        
        if (startTs == null || endTs == null) {
            return;
        }
        
        try {
            Date startDate = outputFormat.parse(startTs);
            Date endDate = outputFormat.parse(endTs);
            if (startDate.after(endDate)) {
                return;
            }
        } catch (Exception e) {
            return;
        }
        
        String normEventName = eventName.trim().replaceAll("\\s+", " ");
        
        String locationId = "-1";
        if (!eventLocation.isEmpty()) {
            String standardizedEventLocation = standardizeLocationName(eventLocation);
            String matchedLocationId = findLocationBySubstring(standardizedEventLocation);
            if (matchedLocationId != null) {
                try {
                    Integer.parseInt(matchedLocationId);
                    locationId = matchedLocationId;
                } catch (NumberFormatException e) {
                }
            }
        }
        
        if (!locationId.equals("-1")) {
            try {
                Integer.parseInt(locationId);
            } catch (NumberFormatException e) {
                locationId = "-1";
            }
        }
        
        String output = eventId + "," + normEventName + "," + startTs + "," + endTs + 
                       "," + (eventType.isEmpty() ? "" : eventType.trim()) + "," + locationId + "," + locationId;
        
        context.write(NullWritable.get(), new Text(output));
    }
}

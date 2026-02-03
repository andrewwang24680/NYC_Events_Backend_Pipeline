import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IngestionMapper
        extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text outKey = new Text();
    private static final int EXPECTED_FIELDS = 16;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        String[] fields = parseCSVLine(line);

        if (fields.length < EXPECTED_FIELDS) {
            context.getCounter("Mapper", "TOO_FEW_COLUMNS").increment(1);
            context.getCounter("Mapper", "COLUMNS_" + fields.length).increment(1);
            return;
        }

        if (fields[0].equalsIgnoreCase("year") || 
            fields[4].equalsIgnoreCase("timestamp")) {
            context.getCounter("Mapper", "HEADER_SKIPPED").increment(1);
            return;
        }

        String timestamp = fields[4].replace("T", " ");
        
        String[] kept = new String[] {
                timestamp,  // timestamp
                fields[6],  // origin_station_complex_name
                fields[7],  // origin_latitude
                fields[8],  // origin_longitude
                fields[10], // destination_station_complex_name
                fields[11], // destination_latitude
                fields[12], // destination_longitude
                fields[13]  // estimated_average_ridership
        };

        if (isEmpty(kept[0]) || isEmpty(kept[1]) || 
            isEmpty(kept[2]) || isEmpty(kept[3]) ||
            isEmpty(kept[4]) || isEmpty(kept[5]) ||
            isEmpty(kept[6]) || isEmpty(kept[7])) {
            context.getCounter("Mapper", "MISSING_CRITICAL_FIELDS").increment(1);
            return;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < kept.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            String field = kept[i] != null ? kept[i].trim() : "";
            
            if (field.contains(",") || field.contains("\"")) {
                sb.append('"').append(field.replace("\"", "\"\"")).append('"');
            } else {
                sb.append(field);
            }
        }

        outKey.set(sb.toString());
        context.write(outKey, NullWritable.get());
        context.getCounter("Mapper", "VALID_RECORDS").increment(1);
    }

    private String[] parseCSVLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;
        
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

    private boolean isEmpty(String value) {
        if (value == null || value.trim().isEmpty()) {
            return true;
        }
        String v = value.trim().toUpperCase();
        return v.equals("N/A") || v.equals("NULL") || v.equals("NA");
    }
}

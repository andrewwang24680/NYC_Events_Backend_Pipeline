import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

public class ProfilingFHVMapper extends Mapper<Void, Group, Text, IntWritable> {
    
    private Text outputKey = new Text();
    private IntWritable one = new IntWritable(1);
    
    @Override
    public void map(Void key, Group record, Context context) 
            throws IOException, InterruptedException {
        
        try {
            String pickupDt = getStringValue(record, "pickup_datetime");
            String dropoffDt = getStringValue(record, "dropoff_datetime");
            String tripMiles = getStringValue(record, "trip_miles");
            String puLocation = getStringValue(record, "PULocationID");
            String doLocation = getStringValue(record, "DOLocationID");
            
            outputKey.set("TOTAL_RECORDS");
            context.write(outputKey, one);
            
            if (pickupDt == null || pickupDt.equals("null") || pickupDt.isEmpty()) {
                outputKey.set("NULL_PICKUP_DATETIME");
                context.write(outputKey, one);
            } else if (!ValidationUtils.isValidDateTime(pickupDt)) {
                outputKey.set("INVALID_PICKUP_DATETIME");
                context.write(outputKey, one);
            } else {
                outputKey.set("VALID_PICKUP_DATETIME");
                context.write(outputKey, one);
            }
            
            if (dropoffDt == null || dropoffDt.equals("null") || dropoffDt.isEmpty()) {
                outputKey.set("NULL_DROPOFF_DATETIME");
                context.write(outputKey, one);
            } else if (!ValidationUtils.isValidDateTime(dropoffDt)) {
                outputKey.set("INVALID_DROPOFF_DATETIME");
                context.write(outputKey, one);
            } else {
                outputKey.set("VALID_DROPOFF_DATETIME");
                context.write(outputKey, one);
            }
            
            if (tripMiles == null || tripMiles.equals("null") || tripMiles.isEmpty()) {
                outputKey.set("NULL_TRIP_MILES");
                context.write(outputKey, one);
            } else if (!ValidationUtils.isValidNumber(tripMiles, 0.0, 100.0)) {
                outputKey.set("INVALID_TRIP_MILES");
                context.write(outputKey, one);
            } else {
                outputKey.set("VALID_TRIP_MILES");
                context.write(outputKey, one);
            }
            
            if (!ValidationUtils.isValidLocationId(puLocation)) {
                outputKey.set("INVALID_PU_LOCATION");
                context.write(outputKey, one);
            } else {
                outputKey.set("VALID_PU_LOCATION");
                context.write(outputKey, one);
            }
            
            if (!ValidationUtils.isValidLocationId(doLocation)) {
                outputKey.set("INVALID_DO_LOCATION");
                context.write(outputKey, one);
            } else {
                outputKey.set("VALID_DO_LOCATION");
                context.write(outputKey, one);
            }
            
        } catch (Exception e) {
            System.err.println("PARSE_ERROR: " + e.getMessage());
        }
    }
    
    private String getStringValue(Group group, String fieldName) {
        try {
            if (group.getType().containsField(fieldName)) {
                int fieldIndex = group.getType().getFieldIndex(fieldName);
                int repetitionCount = group.getFieldRepetitionCount(fieldIndex);
                if (repetitionCount > 0) {
                    try {
                        return group.getString(fieldIndex, 0);
                    } catch (Exception e1) {
                        try {
                            return String.valueOf(group.getInteger(fieldIndex, 0));
                        } catch (Exception e2) {
                            try {
                                return String.valueOf(group.getLong(fieldIndex, 0));
                            } catch (Exception e3) {
                                try {
                                    return String.valueOf(group.getDouble(fieldIndex, 0));
                                } catch (Exception e4) {
                                    try {
                                        return String.valueOf(group.getFloat(fieldIndex, 0));
                                    } catch (Exception e5) {
                                        return null;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
        }
        return null;
    }
}
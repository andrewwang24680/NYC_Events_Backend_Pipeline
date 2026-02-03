import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

public class ProfilingYellowMapper extends Mapper<Void, Group, Text, IntWritable> {
    
    private Text outputKey = new Text();
    private IntWritable one = new IntWritable(1);
    
    @Override
    public void map(Void key, Group record, Context context) 
            throws IOException, InterruptedException {
        
        try {
            String pickupDt = getStringValue(record, "tpep_pickup_datetime");
            String dropoffDt = getStringValue(record, "tpep_dropoff_datetime");
            String passengerCount = getStringValue(record, "passenger_count");
            String tripDistance = getStringValue(record, "trip_distance");
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
            
            if (passengerCount == null || passengerCount.equals("null") || passengerCount.isEmpty()) {
                outputKey.set("NULL_PASSENGER_COUNT");
                context.write(outputKey, one);
            } else if (!ValidationUtils.isValidNumber(passengerCount, 1.0, 6.0)) {
                outputKey.set("INVALID_PASSENGER_COUNT");
                context.write(outputKey, one);
            } else {
                outputKey.set("VALID_PASSENGER_COUNT");
                context.write(outputKey, one);
            }
            
            if (tripDistance == null || tripDistance.equals("null") || tripDistance.isEmpty()) {
                outputKey.set("NULL_TRIP_DISTANCE");
                context.write(outputKey, one);
            } else if (!ValidationUtils.isValidNumber(tripDistance, 0.0, 100.0)) {
                outputKey.set("INVALID_TRIP_DISTANCE");
                context.write(outputKey, one);
            } else {
                outputKey.set("VALID_TRIP_DISTANCE");
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
            System.err.println("Error reading field " + fieldName + ": " + e.getMessage());
        }
        return null;
    }
}
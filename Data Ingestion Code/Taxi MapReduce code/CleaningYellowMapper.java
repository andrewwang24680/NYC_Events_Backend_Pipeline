import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.json.JSONObject;

public class CleaningYellowMapper extends Mapper<Void, Group, Text, Text> {
    
    private static long totalRecords = 0;
    private static long validRecords = 0;
    private static long invalidRecords = 0;
    
    private Text outputKey = new Text("VALID");
    private Text outputValue = new Text();
    
    @Override
    public void map(Void key, Group record, Context context) 
            throws IOException, InterruptedException {
        
        totalRecords++;
        
        try {
            String pickupDt = getStringValue(record, "tpep_pickup_datetime");
            String dropoffDt = getStringValue(record, "tpep_dropoff_datetime");
            String passengerCount = getStringValue(record, "passenger_count");
            String tripDistance = getStringValue(record, "trip_distance");
            String puLocation = getStringValue(record, "PULocationID");
            String doLocation = getStringValue(record, "DOLocationID");
            
            boolean valid = true;
            
            if (!ValidationUtils.isValidNumber(passengerCount, 1.0, 6.0)) {
                valid = false;
            }
            if (!ValidationUtils.isValidNumber(tripDistance, 0.0, 100.0)) {
                valid = false;
            }
            if (!ValidationUtils.isValidLocationId(puLocation)) {
                valid = false;
            }
            if (!ValidationUtils.isValidLocationId(doLocation)) {
                valid = false;
            }
            
            if (valid) {
                JSONObject cleanedRecord = new JSONObject();
                cleanedRecord.put("tpep_pickup_datetime", pickupDt);
                cleanedRecord.put("tpep_dropoff_datetime", dropoffDt);
                cleanedRecord.put("passenger_count", Integer.parseInt(passengerCount));
                cleanedRecord.put("trip_distance", Double.parseDouble(tripDistance));
                cleanedRecord.put("PULocationID", Integer.parseInt(puLocation));
                cleanedRecord.put("DOLocationID", Integer.parseInt(doLocation));
                cleanedRecord.put("taxi_type", "yellow");
                
                outputValue.set(cleanedRecord.toString());
                context.write(outputKey, outputValue);
                validRecords++;
            } else {
                invalidRecords++;
            }
            
        } catch (Exception e) {
            invalidRecords++;
            System.err.println("Error processing record: " + e.getMessage());
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
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("Total records processed: " + totalRecords);
        System.err.println("Valid records: " + validRecords);
        System.err.println("Invalid records: " + invalidRecords);
    }
}
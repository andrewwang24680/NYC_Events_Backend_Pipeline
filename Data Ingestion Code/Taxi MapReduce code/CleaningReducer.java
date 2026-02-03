import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleaningReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text outputKey = new Text();
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        // Only process VALID records
        if (key.toString().equals("VALID")) {
            for (Text value : values) {
                outputKey.set("");
                context.write(outputKey, value);
            }
        }
    }
}
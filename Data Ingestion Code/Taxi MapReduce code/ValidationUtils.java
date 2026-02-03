import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class ValidationUtils {
    
    public static boolean isValidDateTime(String dtStr) {
        try {
            if (dtStr == null || dtStr.equals("null") || dtStr.isEmpty()) {
                return false;
            }
            
            String normalizedDt = dtStr.replace("Z", "+00:00");
            LocalDateTime dt = LocalDateTime.parse(normalizedDt, 
                DateTimeFormatter.ISO_DATE_TIME);
            
            int year = dt.getYear();
            return year >= 2024 && year <= 2025;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
    
    public static boolean isValidNumber(String value, Double minVal, Double maxVal) {
        try {
            if (value == null || value.equals("null") || value.isEmpty()) {
                return false;
            }
            
            double num = Double.parseDouble(value);
            
            if (minVal != null && num < minVal) {
                return false;
            }
            if (maxVal != null && num > maxVal) {
                return false;
            }
            
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    public static boolean isValidLocationId(String locId) {
        try {
            if (locId == null || locId.equals("null") || locId.isEmpty()) {
                return false;
            }
            
            int loc = Integer.parseInt(locId);
            return loc >= 1 && loc <= 263;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    public static boolean isDropoffAfterPickup(String pickupDt, String dropoffDt) {
        try {
            String normalizedPickup = pickupDt.replace("Z", "+00:00");
            String normalizedDropoff = dropoffDt.replace("Z", "+00:00");
            
            LocalDateTime pickup = LocalDateTime.parse(normalizedPickup, 
                DateTimeFormatter.ISO_DATE_TIME);
            LocalDateTime dropoff = LocalDateTime.parse(normalizedDropoff, 
                DateTimeFormatter.ISO_DATE_TIME);
            
            return dropoff.isAfter(pickup);
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
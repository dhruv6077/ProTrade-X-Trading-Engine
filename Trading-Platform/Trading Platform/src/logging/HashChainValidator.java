package logging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Validates the integrity of the audit log hash chain.
 * Can detect if any log entries have been tampered with.
 */
public class HashChainValidator {
    private final MessageDigest digest;
    private final Gson gson = new Gson();
    
    public HashChainValidator() {
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }
    
    /**
     * Validates the hash chain in an audit log file.
     * 
     * @param logFilePath Path to the audit log file
     * @return ValidationResult containing validation status and any errors
     */
    public ValidationResult validateLogFile(String logFilePath) {
        List<String> errors = new ArrayList<>();
        int lineNumber = 0;
        String expectedPreviousHash = "0";
        
        try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                
                // Skip empty lines
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                try {
                    JsonObject json = gson.fromJson(line, JsonObject.class);
                    
                    String eventId = json.get("eventId").getAsString();
                    String previousHash = json.get("previousHash").getAsString();
                    String recordedHash = json.get("hash").getAsString();
                    
                    // Validate previous hash matches expected
                    if (!previousHash.equals(expectedPreviousHash)) {
                        errors.add(String.format("Line %d (Event %s): Previous hash mismatch. Expected: %s, Found: %s",
                                lineNumber, eventId, expectedPreviousHash, previousHash));
                    }
                    
                    // Recalculate hash and validate
                    String calculatedHash = calculateHashFromJson(json, previousHash);
                    if (!calculatedHash.equals(recordedHash)) {
                        errors.add(String.format("Line %d (Event %s): Hash mismatch. Expected: %s, Found: %s",
                                lineNumber, eventId, calculatedHash, recordedHash));
                    }
                    
                    // Update expected previous hash for next entry
                    expectedPreviousHash = recordedHash;
                    
                } catch (Exception e) {
                    errors.add(String.format("Line %d: Error parsing JSON - %s", lineNumber, e.getMessage()));
                }
            }
        } catch (IOException e) {
            errors.add("Error reading log file: " + e.getMessage());
            return new ValidationResult(false, errors, 0);
        }
        
        boolean isValid = errors.isEmpty();
        return new ValidationResult(isValid, errors, lineNumber);
    }
    
    /**
     * Recalculates hash from JSON object.
     */
    private String calculateHashFromJson(JsonObject json, String previousHash) {
        // Reconstruct the data that was hashed
        JsonObject dataForHashing = new JsonObject();
        dataForHashing.add("eventId", json.get("eventId"));
        dataForHashing.add("eventType", json.get("eventType"));
        dataForHashing.add("timestamp", json.get("timestamp"));
        dataForHashing.add("userId", json.get("userId"));
        dataForHashing.add("product", json.get("product"));
        dataForHashing.add("data", json.get("data"));
        
        String dataToHash = previousHash + gson.toJson(dataForHashing);
        return calculateHash(dataToHash);
    }
    
    /**
     * Calculates SHA-256 hash of the input string.
     */
    private String calculateHash(String input) {
        byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(hashBytes);
    }
    
    /**
     * Converts byte array to hexadecimal string.
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
    
    /**
     * Result of hash chain validation.
     */
    public static class ValidationResult {
        private final boolean valid;
        private final List<String> errors;
        private final int totalLines;
        
        public ValidationResult(boolean valid, List<String> errors, int totalLines) {
            this.valid = valid;
            this.errors = errors;
            this.totalLines = totalLines;
        }
        
        public boolean isValid() {
            return valid;
        }
        
        public List<String> getErrors() {
            return new ArrayList<>(errors);
        }
        
        public int getTotalLines() {
            return totalLines;
        }
        
        @Override
        public String toString() {
            if (valid) {
                return String.format("Validation PASSED: %d log entries validated successfully", totalLines);
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("Validation FAILED: %d errors found in %d log entries\n", 
                        errors.size(), totalLines));
                for (String error : errors) {
                    sb.append("  - ").append(error).append("\n");
                }
                return sb.toString();
            }
        }
    }
}

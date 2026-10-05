package gov.nih.nci.hpc.dmesync.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Hl7Util {

    private static final Pattern SEQUENCING_CENTER_PATTERN = Pattern.compile(
            "sequencing files generated at\\s+(.+?)[.]?$",
            Pattern.CASE_INSENSITIVE
    );

    private Hl7Util() {
        // Utility class
    }

    public static String getSex(String hl7Message) {
        String[] fields = getSegmentFields(hl7Message, "PID");

        if (fields != null && fields.length > 8) {
            return emptyToNull(fields[8]); // PID-8
        }

        return null;
    }

    public static String getOriginalSequencingDate(String hl7Message) {
        String[] fields = getSegmentFields(hl7Message, "OBR");

        if (fields != null && fields.length > 7) {
            return emptyToNull(fields[7]); // OBR-7
        }

        return null;
    }

    public static String getSequencingCenter(String hl7Message) {
        if (hl7Message == null || hl7Message.isBlank()) {
            return null;
        }

        String[] segments = hl7Message.split("\\r\\n|\\r|\\n");

        for (String segment : segments) {
            if (!segment.startsWith("OBX|")) {
                continue;
            }

            String[] fields = segment.split("\\|", -1);

            for (String field : fields) {
                Matcher matcher = SEQUENCING_CENTER_PATTERN.matcher(field.trim());

                if (matcher.find()) {
                    return matcher.group(1).trim();
                }
            }
        }

        return null;
    }

    private static String[] getSegmentFields(
            String hl7Message,
            String segmentName) {

        if (hl7Message == null || hl7Message.isBlank()) {
            return null;
        }

        String[] segments = hl7Message.split("\\r\\n|\\r|\\n");

        for (String segment : segments) {
            if (segment.startsWith(segmentName + "|")) {
                return segment.split("\\|", -1);
            }
        }

        return null;
    }

    private static String emptyToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        return value.trim();
    }
}

/*import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.util.Terser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Hl7Util {

    private static final Pattern SEQUENCING_CENTER_PATTERN =
            Pattern.compile(
                    "sequencing files generated at\\s+(.+?)[.]?$",
                    Pattern.CASE_INSENSITIVE
            );

    private Hl7Util() {
        // Utility class
    }

    public static Hl7Metadata extractMetadata(String hl7Message)  {

        try (HapiContext context = new DefaultHapiContext()) {

            Parser parser = context.getPipeParser();
            Message message = parser.parse(hl7Message);

            Terser terser = new Terser(message);

            // Standard HL7 fields
            String sex = getValue(terser, "/PID-8");
            String originalSequencingDate = getValue(terser, "/OBR-7");

            // Organization-specific free-text extraction
            String sequencingCenter =
                    extractSequencingCenter(hl7Message);

            return new Hl7Metadata(
                    sex,
                    originalSequencingDate,
                    sequencingCenter
            );
        } catch (Exception e) {
			// Log the error and return null metadata
			System.err.println("Error parsing HL7 message: " + e.getMessage());
			return new Hl7Metadata(null, null, null);
		}
    }

    private static String getValue(Terser terser, String path) {
        try {
            String value = terser.get(path);

            return value == null || value.isBlank()
                    ? null
                    : value.trim();

        } catch (Exception e) {
            return null;
        }
    }

    private static String extractSequencingCenter(String hl7Message) {

        if (hl7Message == null || hl7Message.isBlank()) {
            return null;
        }

        String[] segments = hl7Message.split("\\r\\n|\\r|\\n");

        for (String segment : segments) {

            if (!segment.startsWith("OBX|")) {
                continue;
            }

            String[] fields = segment.split("\\|", -1);

            for (String field : fields) {

                Matcher matcher =
                        SEQUENCING_CENTER_PATTERN.matcher(field.trim());

                if (matcher.find()) {
                    return matcher.group(1).trim();
                }
            }
        }

        return null;
    }

    public record Hl7Metadata(
            String sex,
            String originalSequencingDate,
            String sequencingCenter
    ) {
    }
}*/


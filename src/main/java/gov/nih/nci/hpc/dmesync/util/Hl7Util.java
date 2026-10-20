package gov.nih.nci.hpc.dmesync.util;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Hl7Util {

    private static final Pattern SEQUENCING_CENTER_PATTERN =
            Pattern.compile(
                    "sequencing files generated at\\s+(.+?)[.]?$",
                    Pattern.CASE_INSENSITIVE
            );

    private Hl7Util() {
    }

    public static Hl7Metadata parse(String hl7) {

        String sex = null;
        String dob = null;
        Integer age = null;
        String orderNumber = null;
        String specimenSource = null;
        String originalSequencingDate = null;
        String sequencingCenter = null;

        String[] segments = hl7.split("\\r\\n|\\r|\\n");

        for (String segment : segments) {

            if (segment.isBlank()) {
                continue;
            }

            String[] fields = segment.split("\\|", -1);

            switch (fields[0]) {

                case "PID" -> {
                    dob = get(fields, 7);
                    sex = get(fields, 8);
                }

                case "ORC" -> {
                    orderNumber = get(fields, 2);
                }

                case "OBR" -> {
                    originalSequencingDate = get(fields, 7);
                    specimenSource = get(fields, 15);
                }

                case "OBX" -> {
                    sequencingCenter =
                            extractSequencingCenter(fields);
                }
            }
        }

        age = calculateAge(dob, originalSequencingDate);

        return new Hl7Metadata(
                sex,
                dob,
                age,
                orderNumber,
                specimenSource,
                originalSequencingDate,
                sequencingCenter
        );
    }

    private static String get(String[] fields, int index) {
        if (index >= fields.length) {
            return null;
        }

        String value = fields[index];

        return value == null || value.isBlank()
                ? null
                : value.trim();
    }

    private static String extractSequencingCenter(
            String[] fields) {

        for (String field : fields) {

            Matcher matcher =
                    SEQUENCING_CENTER_PATTERN.matcher(field.trim());

            if (matcher.find()) {
                return matcher.group(1).trim();
            }
        }

        return null;
    }

    private static Integer calculateAge(
            String dob,
            String referenceDate) {

        if (dob == null) {
            return null;
        }

        try {
        	DateTimeFormatter formatter =
        	        DateTimeFormatter.ofPattern("M/d/yyyy");

            LocalDate birthDate =
                    LocalDate.parse(dob, formatter);

            LocalDate asOfDate =
                    referenceDate != null
                            ? LocalDate.parse(
                                    referenceDate,
                                    formatter)
                            : LocalDate.now();

            return Period.between(
                    birthDate,
                    asOfDate
            ).getYears();

        } catch (Exception e) {
            return null;
        }
    }
    
    public record Hl7Metadata(
            String sex,
            String dateOfBirth,
            Integer age,
            String orderNumber,
            String specimenSource,
            String originalSequencingDate,
            String sequencingCenter
    ) {
    }
}


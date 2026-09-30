package gov.nih.nci.hpc.dmesync.service;

import java.time.LocalDate;

public interface IncludePatternGenerator {
    String getStrategyName();
    String generate(LocalDate currentDate);
}

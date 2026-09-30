package gov.nih.nci.hpc.dmesync.service.impl;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.service.IncludePatternGenerator;

@Component
public class PreviousMonthYyMmDualPrefixIncludePatternGenerator implements IncludePatternGenerator {

    public static final String STRATEGY_NAME = "previous-month-yymm-dual-prefix";
    private static final DateTimeFormatter YYMM_FORMATTER = DateTimeFormatter.ofPattern("yyMM");

    @Override
    public String getStrategyName() {
        return STRATEGY_NAME;
    }

    @Override
    public String generate(LocalDate currentDate) {
        String monthToken = currentDate.minusDays(currentDate.getDayOfMonth()).format(YYMM_FORMATTER);
        return monthToken + "*/**," + "20" + monthToken + "*/**";
    }
}

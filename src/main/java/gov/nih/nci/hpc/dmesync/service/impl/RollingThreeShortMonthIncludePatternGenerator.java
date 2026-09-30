package gov.nih.nci.hpc.dmesync.service.impl;

import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.service.IncludePatternGenerator;

@Component
public class RollingThreeShortMonthIncludePatternGenerator implements IncludePatternGenerator {

    public static final String STRATEGY_NAME = "rolling-3-short-months-lowercase";

    @Override
    public String getStrategyName() {
        return STRATEGY_NAME;
    }

    @Override
    public String generate(LocalDate currentDate) {
        return Stream.of(currentDate.minusMonths(2), currentDate.minusMonths(1), currentDate)
            .map(date -> date.getMonth().getDisplayName(TextStyle.SHORT, Locale.ENGLISH).toLowerCase(Locale.ENGLISH) + "/**")
            .collect(Collectors.joining(","));
    }
}

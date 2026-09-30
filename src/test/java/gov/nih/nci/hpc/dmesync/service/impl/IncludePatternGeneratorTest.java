package gov.nih.nci.hpc.dmesync.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

class IncludePatternGeneratorTest {

  @Test
  void rollingThreeShortMonthGeneratorWrapsAcrossYear() {
    RollingThreeShortMonthIncludePatternGenerator generator = new RollingThreeShortMonthIncludePatternGenerator();

    assertEquals("nov/**,dec/**,jan/**", generator.generate(LocalDate.of(2026, 1, 15)));
  }

  @Test
  void previousMonthYyMmGeneratorMatchesLegacyScriptStyle() {
    PreviousMonthYyMmDualPrefixIncludePatternGenerator generator = new PreviousMonthYyMmDualPrefixIncludePatternGenerator();

    assertEquals("2608*/**,202608*/**", generator.generate(LocalDate.of(2026, 9, 1)));
  }
}

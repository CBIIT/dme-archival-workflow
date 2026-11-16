package gov.nih.nci.hpc.dmesync.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

class IncludePatternStrategyTest {

  @Test
  void rollingThreeShortMonthsWrapsAcrossYear() {
    assertEquals("nov/**,dec/**,jan/**",
        IncludePatternStrategy.ROLLING_3_SHORT_MONTHS_LOWERCASE.generate(LocalDate.of(2026, 1, 15)));
  }

  @Test
  void previousMonthYyMmMatchesLegacyScriptStyle() {
    assertEquals("2608*/**,202608*/**",
        IncludePatternStrategy.PREVIOUS_MONTH_YYMM_DUAL_PREFIX.generate(LocalDate.of(2026, 9, 1)));
  }

  @Test
  void findResolvesStableNamesIgnoringCaseAndWhitespace() {
    assertEquals(IncludePatternStrategy.PREVIOUS_MONTH_YYMM_DUAL_PREFIX,
        IncludePatternStrategy.find("  Previous-Month-YYMM-Dual-Prefix ").orElseThrow());
    assertFalse(IncludePatternStrategy.find("unknown").isPresent());
    assertFalse(IncludePatternStrategy.find(null).isPresent());
    assertTrue(IncludePatternStrategy.find("rolling-3-short-months-lowercase").isPresent());
  }
}

package gov.nih.nci.hpc.dmesync.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.service.DocConfigService;
import gov.nih.nci.hpc.dmesync.service.IncludePatternStrategy;

class DmeSyncSchedulerIncludePatternCheckTest {

  @Test
  void refreshMonthlyIncludePatternIfEnabledUpdatesWhenPatternChanged() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    ReflectionTestUtils.setField(scheduler, "configService", configService);

    DocConfig currentConfig = docConfig("old/**", true, IncludePatternStrategy.ROLLING_3_SHORT_MONTHS_LOWERCASE);
    DocConfig refreshedConfig = docConfig("jul/**,aug/**,sep/**", true, IncludePatternStrategy.ROLLING_3_SHORT_MONTHS_LOWERCASE);

    when(configService.getDocConfigByName("csb")).thenReturn(Optional.of(currentConfig), Optional.of(refreshedConfig));
    when(configService.updateSourceIncludePattern(42L, "jul/**,aug/**,sep/**")).thenReturn(true);

    DocConfig result = (DocConfig) ReflectionTestUtils.invokeMethod(
        scheduler, "refreshMonthlyIncludePatternIfEnabled", currentConfig, LocalDate.of(2026, 9, 1));

    verify(configService).updateSourceIncludePattern(42L, "jul/**,aug/**,sep/**");
    assertEquals("jul/**,aug/**,sep/**", result.getSourceRule().getIncludePattern());
  }

  @Test
  void refreshMonthlyIncludePatternIfEnabledSkipsWhenPatternAlreadyCurrent() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    ReflectionTestUtils.setField(scheduler, "configService", configService);

    DocConfig config = docConfig("jul/**,aug/**,sep/**", true, IncludePatternStrategy.ROLLING_3_SHORT_MONTHS_LOWERCASE);
    when(configService.getDocConfigByName("csb")).thenReturn(Optional.of(config));

    DocConfig result = (DocConfig) ReflectionTestUtils.invokeMethod(
        scheduler, "refreshMonthlyIncludePatternIfEnabled", config, LocalDate.of(2026, 9, 1));

    verify(configService, never()).updateSourceIncludePattern(anyLong(), eq("jul/**,aug/**,sep/**"));
    assertEquals("jul/**,aug/**,sep/**", result.getSourceRule().getIncludePattern());
  }

  @Test
  void refreshMonthlyIncludePatternIfEnabledSkipsWhenFlagDisabled() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    ReflectionTestUtils.setField(scheduler, "configService", configService);

    DocConfig config = docConfig("unchanged/**", false, null);
    when(configService.getDocConfigByName("csb")).thenReturn(Optional.of(config));

    DocConfig result = (DocConfig) ReflectionTestUtils.invokeMethod(
        scheduler, "refreshMonthlyIncludePatternIfEnabled", config, LocalDate.of(2026, 9, 1));

    verify(configService, never()).updateSourceIncludePattern(anyLong(), eq("jul/**,aug/**,sep/**"));
    assertEquals("unchanged/**", result.getSourceRule().getIncludePattern());
  }

  private static DocConfig docConfig(String includePattern, boolean autoUpdate, IncludePatternStrategy strategy) {
    DocConfig.SourceRule sourceRule = new DocConfig.SourceRule(
        null, includePattern, autoUpdate, strategy == null ? null : strategy.getStrategyName(),
        null, null, null, false, false, null, null, false, null, false, false, false, 1);
    return new DocConfig(
        42L, "csb", null, null, null, null, null, true, null, 1, Instant.now(), Instant.now(),
        null, sourceRule, null, null, null, null);
  }
}

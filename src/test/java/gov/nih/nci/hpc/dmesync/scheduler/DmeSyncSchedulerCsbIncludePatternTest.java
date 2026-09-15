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

class DmeSyncSchedulerCsbIncludePatternTest {

  @Test
  void buildRollingThreeMonthIncludePatternWrapsAcrossYear() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();

    String pattern = (String) ReflectionTestUtils.invokeMethod(
        scheduler,
        "buildRollingThreeMonthIncludePattern",
        LocalDate.of(2026, 1, 15));

    assertEquals("nov/**,dec/**,jan/**", pattern);
  }

  @Test
  void refreshCsbMonthlyIncludePatternUpdatesChangedPattern() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    ReflectionTestUtils.setField(scheduler, "configService", configService);

    DocConfig.SourceRule sourceRule = new DocConfig.SourceRule(
        null, "old/**", null, null, null, false, false, null, null, false, null, false, false, false, 1);
    DocConfig config = new DocConfig(
        42L, "csb", null, null, null, null, null, true, null, 1, Instant.now(), Instant.now(),
        null, sourceRule, null, null, null, null);

    when(configService.getDocConfigByName("csb")).thenReturn(Optional.of(config));
    when(configService.updateSourceIncludePattern(42L, "jul/**,aug/**,sep/**")).thenReturn(true);

    ReflectionTestUtils.invokeMethod(
        scheduler,
        "refreshCsbMonthlyIncludePattern",
        LocalDate.of(2026, 9, 10));

    verify(configService).updateSourceIncludePattern(42L, "jul/**,aug/**,sep/**");
  }

  @Test
  void refreshCsbMonthlyIncludePatternSkipsWhenPatternAlreadyCurrent() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    ReflectionTestUtils.setField(scheduler, "configService", configService);

    DocConfig.SourceRule sourceRule = new DocConfig.SourceRule(
        null, "jul/**,aug/**,sep/**", null, null, null, false, false, null, null, false, null, false, false, false, 1);
    DocConfig config = new DocConfig(
        42L, "csb", null, null, null, null, null, true, null, 1, Instant.now(), Instant.now(),
        null, sourceRule, null, null, null, null);

    when(configService.getDocConfigByName("csb")).thenReturn(Optional.of(config));

    ReflectionTestUtils.invokeMethod(
        scheduler,
        "refreshCsbMonthlyIncludePattern",
        LocalDate.of(2026, 9, 10));

    verify(configService, never()).updateSourceIncludePattern(anyLong(), eq("jul/**,aug/**,sep/**"));
  }

  @Test
  void refreshCsbMonthlyIncludePatternSkipsWhenConfigMissing() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    ReflectionTestUtils.setField(scheduler, "configService", configService);

    when(configService.getDocConfigByName("csb")).thenReturn(Optional.empty());

    ReflectionTestUtils.invokeMethod(
        scheduler,
        "refreshCsbMonthlyIncludePattern",
        LocalDate.of(2026, 9, 10));

    verify(configService, never()).updateSourceIncludePattern(anyLong(), eq("jul/**,aug/**,sep/**"));
  }
}

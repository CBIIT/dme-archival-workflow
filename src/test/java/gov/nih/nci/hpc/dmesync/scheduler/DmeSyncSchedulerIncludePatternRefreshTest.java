package gov.nih.nci.hpc.dmesync.scheduler;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.service.DocConfigService;
import gov.nih.nci.hpc.dmesync.service.IncludePatternGenerator;
import gov.nih.nci.hpc.dmesync.service.IncludePatternGeneratorRegistry;
import gov.nih.nci.hpc.dmesync.service.ScheduledJobLockService;
import gov.nih.nci.hpc.dmesync.service.impl.RollingThreeShortMonthIncludePatternGenerator;

class DmeSyncSchedulerIncludePatternRefreshTest {

  @Test
  void refreshMonthlyIncludePatternsSkipsWhenLockNotAcquired() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    ScheduledJobLockService lockService = mock(ScheduledJobLockService.class);

    ReflectionTestUtils.setField(scheduler, "configService", configService);
    ReflectionTestUtils.setField(scheduler, "scheduledJobLockService", lockService);

    when(lockService.tryAcquire("include-pattern-refresh", "2026-09")).thenReturn(false);

    ReflectionTestUtils.invokeMethod(
        scheduler,
        "refreshMonthlyIncludePatterns",
        LocalDate.of(2026, 9, 1));

    verify(configService, never()).getDocConfigsWithIncludePatternAutoUpdate();
    verify(lockService, never()).markCompleted("include-pattern-refresh", "2026-09", true);
  }

  @Test
  void refreshMonthlyIncludePatternsUpdatesDocUsingConfiguredStrategy() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    ScheduledJobLockService lockService = mock(ScheduledJobLockService.class);
    IncludePatternGeneratorRegistry registry = mock(IncludePatternGeneratorRegistry.class);
    IncludePatternGenerator generator = mock(IncludePatternGenerator.class);

    ReflectionTestUtils.setField(scheduler, "configService", configService);
    ReflectionTestUtils.setField(scheduler, "scheduledJobLockService", lockService);
    ReflectionTestUtils.setField(scheduler, "includePatternGeneratorRegistry", registry);

    DocConfig.SourceRule sourceRule = new DocConfig.SourceRule(
        null, "old/**", true, RollingThreeShortMonthIncludePatternGenerator.STRATEGY_NAME,
        null, null, null, false, false, null, null, false, null, false, false, false, 1);
    DocConfig config = new DocConfig(
        42L, "csb", null, null, null, null, null, true, null, 1, Instant.now(), Instant.now(),
        null, sourceRule, null, null, null, null);

    when(lockService.tryAcquire("include-pattern-refresh", "2026-09")).thenReturn(true);
    when(configService.getDocConfigsWithIncludePatternAutoUpdate()).thenReturn(List.of(config));
    when(registry.find(RollingThreeShortMonthIncludePatternGenerator.STRATEGY_NAME)).thenReturn(Optional.of(generator));
    when(generator.generate(LocalDate.of(2026, 9, 1))).thenReturn("jul/**,aug/**,sep/**");
    when(configService.updateSourceIncludePattern(42L, "jul/**,aug/**,sep/**")).thenReturn(true);

    ReflectionTestUtils.invokeMethod(
        scheduler,
        "refreshMonthlyIncludePatterns",
        LocalDate.of(2026, 9, 1));

    verify(configService).updateSourceIncludePattern(42L, "jul/**,aug/**,sep/**");
    verify(lockService).markCompleted("include-pattern-refresh", "2026-09", true);
  }

  @Test
  void refreshMonthlyIncludePatternsSkipsUpdateWhenPatternAlreadyCurrent() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    ScheduledJobLockService lockService = mock(ScheduledJobLockService.class);
    IncludePatternGeneratorRegistry registry = mock(IncludePatternGeneratorRegistry.class);
    IncludePatternGenerator generator = mock(IncludePatternGenerator.class);

    ReflectionTestUtils.setField(scheduler, "configService", configService);
    ReflectionTestUtils.setField(scheduler, "scheduledJobLockService", lockService);
    ReflectionTestUtils.setField(scheduler, "includePatternGeneratorRegistry", registry);

    DocConfig.SourceRule sourceRule = new DocConfig.SourceRule(
        null, "jul/**,aug/**,sep/**", true, RollingThreeShortMonthIncludePatternGenerator.STRATEGY_NAME,
        null, null, null, false, false, null, null, false, null, false, false, false, 1);
    DocConfig config = new DocConfig(
        42L, "csb", null, null, null, null, null, true, null, 1, Instant.now(), Instant.now(),
        null, sourceRule, null, null, null, null);

    when(lockService.tryAcquire("include-pattern-refresh", "2026-09")).thenReturn(true);
    when(configService.getDocConfigsWithIncludePatternAutoUpdate()).thenReturn(List.of(config));
    when(registry.find(RollingThreeShortMonthIncludePatternGenerator.STRATEGY_NAME)).thenReturn(Optional.of(generator));
    when(generator.generate(LocalDate.of(2026, 9, 1))).thenReturn("jul/**,aug/**,sep/**");

    ReflectionTestUtils.invokeMethod(
        scheduler,
        "refreshMonthlyIncludePatterns",
        LocalDate.of(2026, 9, 1));

    verify(configService, never()).updateSourceIncludePattern(anyLong(), eq("jul/**,aug/**,sep/**"));
    verify(lockService).markCompleted("include-pattern-refresh", "2026-09", true);
  }
}

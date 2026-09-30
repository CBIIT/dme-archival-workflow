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
import gov.nih.nci.hpc.dmesync.service.IncludePatternGenerator;
import gov.nih.nci.hpc.dmesync.service.IncludePatternGeneratorRegistry;
import gov.nih.nci.hpc.dmesync.service.impl.RollingThreeShortMonthIncludePatternGenerator;

class DmeSyncSchedulerIncludePatternCheckTest {

  @Test
  void refreshMonthlyIncludePatternIfEnabledUpdatesWhenPatternChanged() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    IncludePatternGeneratorRegistry registry = mock(IncludePatternGeneratorRegistry.class);
    IncludePatternGenerator generator = mock(IncludePatternGenerator.class);

    ReflectionTestUtils.setField(scheduler, "configService", configService);
    ReflectionTestUtils.setField(scheduler, "includePatternGeneratorRegistry", registry);

    DocConfig.SourceRule currentSourceRule = new DocConfig.SourceRule(
        null, "old/**", true, RollingThreeShortMonthIncludePatternGenerator.STRATEGY_NAME,
        null, null, null, false, false, null, null, false, null, false, false, false, 1);
    DocConfig currentConfig = new DocConfig(
        42L, "csb", null, null, null, null, null, true, null, 1, Instant.now(), Instant.now(),
        null, currentSourceRule, null, null, null, null);

    DocConfig.SourceRule refreshedSourceRule = new DocConfig.SourceRule(
        null, "jul/**,aug/**,sep/**", true, RollingThreeShortMonthIncludePatternGenerator.STRATEGY_NAME,
        null, null, null, false, false, null, null, false, null, false, false, false, 1);
    DocConfig refreshedConfig = new DocConfig(
        42L, "csb", null, null, null, null, null, true, null, 1, Instant.now(), Instant.now(),
        null, refreshedSourceRule, null, null, null, null);

    when(configService.getDocConfigByName("csb")).thenReturn(Optional.of(currentConfig), Optional.of(refreshedConfig));
    when(registry.find(RollingThreeShortMonthIncludePatternGenerator.STRATEGY_NAME)).thenReturn(Optional.of(generator));
    when(generator.generate(LocalDate.of(2026, 9, 1))).thenReturn("jul/**,aug/**,sep/**");
    when(configService.updateSourceIncludePattern(42L, "jul/**,aug/**,sep/**")).thenReturn(true);

    DocConfig result = (DocConfig) ReflectionTestUtils.invokeMethod(
        scheduler,
        "refreshMonthlyIncludePatternIfEnabled",
        currentConfig,
        LocalDate.of(2026, 9, 1));

    verify(configService).updateSourceIncludePattern(42L, "jul/**,aug/**,sep/**");
    assertEquals("jul/**,aug/**,sep/**", result.getSourceRule().getIncludePattern());
  }

  @Test
  void refreshMonthlyIncludePatternIfEnabledSkipsWhenPatternAlreadyCurrent() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    IncludePatternGeneratorRegistry registry = mock(IncludePatternGeneratorRegistry.class);
    IncludePatternGenerator generator = mock(IncludePatternGenerator.class);

    ReflectionTestUtils.setField(scheduler, "configService", configService);
    ReflectionTestUtils.setField(scheduler, "includePatternGeneratorRegistry", registry);

    DocConfig.SourceRule sourceRule = new DocConfig.SourceRule(
        null, "jul/**,aug/**,sep/**", true, RollingThreeShortMonthIncludePatternGenerator.STRATEGY_NAME,
        null, null, null, false, false, null, null, false, null, false, false, false, 1);
    DocConfig config = new DocConfig(
        42L, "csb", null, null, null, null, null, true, null, 1, Instant.now(), Instant.now(),
        null, sourceRule, null, null, null, null);

    when(configService.getDocConfigByName("csb")).thenReturn(Optional.of(config));
    when(registry.find(RollingThreeShortMonthIncludePatternGenerator.STRATEGY_NAME)).thenReturn(Optional.of(generator));
    when(generator.generate(LocalDate.of(2026, 9, 1))).thenReturn("jul/**,aug/**,sep/**");

    DocConfig result = (DocConfig) ReflectionTestUtils.invokeMethod(
        scheduler,
        "refreshMonthlyIncludePatternIfEnabled",
        config,
        LocalDate.of(2026, 9, 1));

    verify(configService, never()).updateSourceIncludePattern(anyLong(), eq("jul/**,aug/**,sep/**"));
    assertEquals("jul/**,aug/**,sep/**", result.getSourceRule().getIncludePattern());
  }

  @Test
  void refreshMonthlyIncludePatternIfEnabledSkipsWhenFlagDisabled() {
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    DocConfigService configService = mock(DocConfigService.class);
    IncludePatternGeneratorRegistry registry = mock(IncludePatternGeneratorRegistry.class);

    ReflectionTestUtils.setField(scheduler, "configService", configService);
    ReflectionTestUtils.setField(scheduler, "includePatternGeneratorRegistry", registry);

    DocConfig.SourceRule sourceRule = new DocConfig.SourceRule(
        null, "unchanged/**", false, null,
        null, null, null, false, false, null, null, false, null, false, false, false, 1);
    DocConfig config = new DocConfig(
        42L, "csb", null, null, null, null, null, true, null, 1, Instant.now(), Instant.now(),
        null, sourceRule, null, null, null, null);

    when(configService.getDocConfigByName("csb")).thenReturn(Optional.of(config));

    DocConfig result = (DocConfig) ReflectionTestUtils.invokeMethod(
        scheduler,
        "refreshMonthlyIncludePatternIfEnabled",
        config,
        LocalDate.of(2026, 9, 1));

    verify(registry, never()).find(eq(RollingThreeShortMonthIncludePatternGenerator.STRATEGY_NAME));
    verify(configService, never()).updateSourceIncludePattern(anyLong(), eq("jul/**,aug/**,sep/**"));
    assertEquals("unchanged/**", result.getSourceRule().getIncludePattern());
  }
}

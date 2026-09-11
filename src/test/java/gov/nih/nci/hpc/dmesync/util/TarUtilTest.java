package gov.nih.nci.hpc.dmesync.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TarUtilTest {

  @Test
  void legacyGroupingPreservesExistingDelimiterLevelBehavior() {
    assertEquals("1_10", TarUtil.buildBatchGroupKey("1_10_and_189", "_", 2, "legacy").orElseThrow());
    assertEquals("1_10", TarUtil.buildBatchGroupKey("1_10_and_167", "_", 2, "legacy").orElseThrow());
  }

  @Test
  void andPrefixGroupingUsesPrefixAndFirstSuffixDigit() {
    assertEquals("1_10_and_1", TarUtil.buildBatchGroupKey("1_10_and_189", "_", 2, "and-prefix").orElseThrow());
    assertEquals("1_10_and_1", TarUtil.buildBatchGroupKey("1_10_and_167", "_", 2, "and-prefix").orElseThrow());
    assertEquals("1_10_and_2", TarUtil.buildBatchGroupKey("1_10_and_245", "_", 2, "and-prefix").orElseThrow());
  }

  @Test
  void unsupportedGroupingModeIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> TarUtil.buildBatchGroupKey("1_10_and_189", "_", 2, "unknown"));
  }
}

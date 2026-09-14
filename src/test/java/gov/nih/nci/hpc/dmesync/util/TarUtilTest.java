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
  void andPrefixGroupingUsesPrefixAndFirstTwoSuffixCharacters() {
    assertEquals("CDK6_Q00534_and_AA",
        TarUtil.buildBatchGroupKey("CDK6_Q00534_and_AA123", "_", 2, "and-prefix").orElseThrow());
    assertEquals("CDK6_Q00534_and_AB",
        TarUtil.buildBatchGroupKey("CDK6_Q00534_and_AB999", "_", 2, "and-prefix").orElseThrow());
    assertEquals("CDK6_Q00534_and_BA",
        TarUtil.buildBatchGroupKey("CDK6_Q00534_and_BA777", "_", 2, "and-prefix").orElseThrow());
  }

  @Test
  void unsupportedGroupingModeIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> TarUtil.buildBatchGroupKey("1_10_and_189", "_", 2, "unknown"));
  }
}

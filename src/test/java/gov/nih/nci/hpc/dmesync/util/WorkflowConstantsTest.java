package gov.nih.nci.hpc.dmesync.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class WorkflowConstantsTest {

  @Test
  void retryableStatusIncludesFailedAndLegacyNullOnly() {
    assertTrue(WorkflowConstants.isRetryableStatus(null));
    assertTrue(WorkflowConstants.isRetryableStatus("FAILED"));
    assertTrue(WorkflowConstants.isRetryableStatus("FAILED.legacy detail"));
    assertFalse(WorkflowConstants.isRetryableStatus("COMPLETED"));
    assertFalse(WorkflowConstants.isRetryableStatus("IGNORED"));
  }

  @Test
  void displayStatusNormalizesLegacyFailures() {
    assertEquals("FAILED", WorkflowConstants.getDisplayStatus(null));
    assertEquals("FAILED", WorkflowConstants.getDisplayStatus("FAILED.detail"));
    assertEquals("COMPLETED", WorkflowConstants.getDisplayStatus("COMPLETED"));
    assertEquals("IGNORED", WorkflowConstants.getDisplayStatus("IGNORED"));
  }

  @Test
  void ignoredRunIdSuffixIsAddedOnlyOnce() {
    assertEquals("Run_1_IGNORED", WorkflowConstants.toIgnoredRunId("Run_1"));
    assertEquals("Run_1_IGNORED", WorkflowConstants.toIgnoredRunId("Run_1_IGNORED"));
  }
}

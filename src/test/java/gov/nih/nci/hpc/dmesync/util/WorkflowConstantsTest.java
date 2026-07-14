package gov.nih.nci.hpc.dmesync.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class WorkflowConstantsTest {

  @Test
  void retryableStatusIncludesFailedAndLegacyNullOnly() {
    assertTrue(WorkflowConstants.isRetryableStatus("FAILED"));
    assertFalse(WorkflowConstants.isRetryableStatus("COMPLETED"));
    assertFalse(WorkflowConstants.isRetryableStatus("IGNORED"));
  }


  @Test
  void ignoredRunIdSuffixIsAddedOnlyOnce() {
    assertEquals("Run_1_IGNORED", WorkflowConstants.toIgnoredRunId("Run_1"));
    assertEquals("Run_1_IGNORED", WorkflowConstants.toIgnoredRunId("Run_1_IGNORED"));
  }
}

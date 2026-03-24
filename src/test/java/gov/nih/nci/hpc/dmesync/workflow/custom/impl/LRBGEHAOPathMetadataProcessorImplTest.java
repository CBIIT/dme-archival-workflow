package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.util.DmeMetadataBuilder;
import gov.nih.nci.hpc.dmesync.workflow.MessageService;

/**
 *  tests for {@link LRBGEHAOPathMetadataProcessorImpl}.
 *
 */
class LRBGEHAOPathMetadataProcessorImplTest {

  @TempDir
  Path tempDir;

  @Test
  void testGetArchivePath_FlowcellRawData() throws Exception {
    LRBGEHAOPathMetadataProcessorImpl processor = new LRBGEHAOPathMetadataProcessorImpl();
    setField(processor, "destinationBaseDir", "/DME");
    setField(processor, "metadataFile", "dummy.xlsx");

    // Build structure: .../Hager/OUT/OUT_20250320/Flowcell/Sample_ABC/reads.fastq.gz
    Path sampleDir = Files.createDirectories(
        tempDir.resolve("Hager/OUT/OUT_20250320/Flowcell/Sample_ABC")
    );
    Path file = Files.createFile(sampleDir.resolve("reads.fastq.gz"));

    // Mock excel map with the exact key production code will look up
    mockExcelForFile(processor, file, Map.of("project_id", "PROJ1"));

    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setOriginalFilePath(file.toString());
    statusInfo.setSourceFilePath(file.toString());
    statusInfo.setSourceFileName("reads.fastq.gz");

    String archivePath = processor.getArchivePath(statusInfo);

    assertEquals("/DME/PI_Gordon_Hager/Project_PROJ1/OUT_20250320/Raw_Data/Sample_ABC/reads.fastq.gz", archivePath);
  }

  @Test
  void testGetArchivePath_Analysis() throws Exception {
    LRBGEHAOPathMetadataProcessorImpl processor = new LRBGEHAOPathMetadataProcessorImpl();
    setField(processor, "destinationBaseDir", "/DME");
    setField(processor, "metadataFile", "dummy.xlsx");

    Path analysisDir = Files.createDirectories(tempDir.resolve("Hager/OUT/OUT_20250320/Analysis"));
    Path file = Files.createFile(analysisDir.resolve("report.tsv"));

    mockExcelForFile(processor, file, Map.of("project_id", "PROJ1"));

    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setOriginalFilePath(file.toString());
    statusInfo.setSourceFilePath(file.toString());
    statusInfo.setSourceFileName("report.tsv");

    String archivePath = processor.getArchivePath(statusInfo);

    assertEquals("/DME/PI_Gordon_Hager/Project_PROJ1/OUT_20250320/Analysis/report.tsv", archivePath);
  }

  @Test
  void testGetArchivePath_QC() throws Exception {
    LRBGEHAOPathMetadataProcessorImpl processor = new LRBGEHAOPathMetadataProcessorImpl();
    setField(processor, "destinationBaseDir", "/DME");
    setField(processor, "metadataFile", "dummy.xlsx");

    Path qcDir = Files.createDirectories(tempDir.resolve("Hager/OUT/OUT_20250320/QC"));
    Path file = Files.createFile(qcDir.resolve("qc.html"));

    mockExcelForFile(processor, file, Map.of("project_id", "PROJ1"));

    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setOriginalFilePath(file.toString());
    statusInfo.setSourceFilePath(file.toString());
    statusInfo.setSourceFileName("qc.html");

    String archivePath = processor.getArchivePath(statusInfo);

    assertEquals("/DME/PI_Gordon_Hager/Project_PROJ1/OUT_20250320/QC/qc.html", archivePath);
  }

  @Test
  void testGetArchivePath_ReadmeUnderOutCollection() throws Exception {
    LRBGEHAOPathMetadataProcessorImpl processor = new LRBGEHAOPathMetadataProcessorImpl();
    setField(processor, "destinationBaseDir", "/DME");
    setField(processor, "metadataFile", "dummy.xlsx");

    Path outRunDir = Files.createDirectories(tempDir.resolve("Hager/OUT/OUT_20250320"));
    Path file = Files.createFile(outRunDir.resolve("readme.txt"));

    mockExcelForFile(processor, file, Map.of("project_id", "PROJ1"));

    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setOriginalFilePath(file.toString());
    statusInfo.setSourceFilePath(file.toString());
    statusInfo.setSourceFileName("readme.txt");

    String archivePath = processor.getArchivePath(statusInfo);

    assertEquals("/DME/PI_Gordon_Hager/Project_PROJ1/OUT_20250320/readme.txt", archivePath);
  }

  @Test
  void testGetArchivePath_ThrowsWhenUnknownSubcollectionType() throws Exception {
    LRBGEHAOPathMetadataProcessorImpl processor = new LRBGEHAOPathMetadataProcessorImpl();
    setField(processor, "destinationBaseDir", "/DME");
    setField(processor, "metadataFile", "dummy.xlsx");

    injectMessageServiceReturning(processor);

    // Must include OUT and OUT_... to avoid NPE in getPathForMetadata()/getCollectionPathFromParent()
    Path unknownDir = Files.createDirectories(
        tempDir.resolve("Hager/OUT/OUT_20250320/UnknownType")
    );
    Path file = Files.createFile(unknownDir.resolve("file.txt"));

    // Provide excel mapping so project_id isn't null (not strictly required for exception, but keeps logs clean)
    mockExcelForFile(processor, file, Map.of("project_id", "PROJ1"));

    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setOriginalFilePath(file.toString());
    statusInfo.setSourceFilePath(file.toString());
    statusInfo.setSourceFileName("file.txt");

    assertThrows(DmeSyncMappingException.class, () -> processor.getArchivePath(statusInfo));
  }

  /**
   * Inject a lightweight {@link MessageService} mock that always returns a constant message.
   * This helper assumes the messageService field is a Spring {@code MessageService} bean
   * with a varargs signature {@code get(String, Object...)}; the stub below exercises it
   * using only the message key argument.
   */
  private static void injectMessageServiceReturning(Object target) throws Exception {
	  MessageService messageService = mock(MessageService.class);

	  // If MessageService has: String get(String key, Object... args)
	  when(messageService.get(anyString())).thenReturn("VALIDATION_001");

	  setField(target, "messageService", messageService);
	}

  @Test
  void testGetPathForMetadata_NormalizesToDataWhenHagerPresent() throws Exception {
    LRBGEHAOPathMetadataProcessorImpl processor = new LRBGEHAOPathMetadataProcessorImpl();

    Path dir = Files.createDirectories(tempDir.resolve("Hager/OUT/OUT_20250320/Analysis"));
    Path file = Files.createFile(dir.resolve("x.txt"));

    String metadataKey = processor.getPathForMetadata(file);
    // Normalize separators for Windows portability.
    // Path#toString() can contain backslashes on Windows, while production code asserts Unix-like segments.
    String normalizedMetadataKey = metadataKey.replace('\\', '/');

    // Example expected: /data/Hager/OUT/OUT_20250320
    assertTrue(normalizedMetadataKey.startsWith("/data/Hager/"), "Expected metadataKey to start with /data/Hager/ but was " + normalizedMetadataKey);
    assertTrue(normalizedMetadataKey.endsWith("/OUT/OUT_20250320"), "Expected metadataKey to end with /OUT/OUT_20250320 but was " + normalizedMetadataKey);
  }

  // ---------------- helpers ----------------

  private static void mockExcelForFile(LRBGEHAOPathMetadataProcessorImpl processor,
                                      Path fileUnderOut,
                                      Map<String, String> rowForThatKey) throws Exception {
    String metadataKey = processor.getPathForMetadata(fileUnderOut);

    Map<String, Map<String, String>> excelMap = new HashMap<>();
    excelMap.put(metadataKey, new HashMap<>(rowForThatKey));

    mockExcelMap(processor, excelMap);
  }

  private static void mockExcelMap(LRBGEHAOPathMetadataProcessorImpl processor,
                                   Map<String, Map<String, String>> excelMap) throws Exception {
    DmeMetadataBuilder dmeMetadataBuilder = mock(DmeMetadataBuilder.class);
    when(dmeMetadataBuilder.getMetadataMap(anyString(), eq("Path"))).thenReturn(excelMap);

    setField(processor, "dmeMetadataBuilder", dmeMetadataBuilder);
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    java.lang.reflect.Field f = findField(target.getClass(), fieldName);
    f.setAccessible(true);
    f.set(target, value);
  }

  private static java.lang.reflect.Field findField(Class<?> type, String fieldName) throws NoSuchFieldException {
    Class<?> t = type;
    while (t != null) {
      try {
        return t.getDeclaredField(fieldName);
      } catch (NoSuchFieldException ignored) {
        t = t.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }
}
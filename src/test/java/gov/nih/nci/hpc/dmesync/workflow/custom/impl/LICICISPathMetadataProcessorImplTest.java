package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.util.DmeMetadataBuilder;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

@RunWith(SpringRunner.class)
@SpringBootTest({
    "hpc.server.url=https://fr-s-hpcdm-gp-d.ncifcrf.gov:7738/hpc-server",
    "auth.token=xxxx"
})
public class LICICISPathMetadataProcessorImplTest {

    @Autowired
    LICICISPathMetadataProcessorImpl liciCisPathMetadataProcessorImpl;

    private static final String DESTINATION_BASE_DIR = "/CCR_LICI_CIS_Archive";
    private static final String PROJECT_ID = "ProjectABC";
    private static final String METADATA_KEY_JAMSARCHIVE =
            "/data/Trinchieri_lab/JAMSarchive/JAMSdb202201";
    private static final String METADATA_KEY_JAMSBETA =
            "/data/Trinchieri_lab/JAMSBeta/JAMSdb202201";

    @Before
    public void init() throws Exception {
        liciCisPathMetadataProcessorImpl.destinationBaseDir = DESTINATION_BASE_DIR;

        // Inject a Mockito mock for DmeMetadataBuilder via reflection
        DmeMetadataBuilder mockMetadataBuilder = Mockito.mock(DmeMetadataBuilder.class);
        when(mockMetadataBuilder.getMetadataMap(any(), any()))
                .thenReturn(buildMetadataMap());

        Field dmeMetadataBuilderField =
                LICICISPathMetadataProcessorImpl.class.getDeclaredField("dmeMetadataBuilder");
        dmeMetadataBuilderField.setAccessible(true);
        dmeMetadataBuilderField.set(liciCisPathMetadataProcessorImpl, mockMetadataBuilder);
    }

    // ---------------------------------------------------------------------------
    // getArchivePath() tests
    // ---------------------------------------------------------------------------

    @Test
    public void testGetArchivePath_reads() throws Exception {
        StatusInfo statusInfo = setupStatusInfo(
                "/data/Trinchieri_lab/JAMSarchive/JAMSdb202201/reads",
                "/data/Trinchieri_lab/JAMSarchive/JAMSdb202201/reads/sample.fastq",
                "sample.fastq");

        String expected = DESTINATION_BASE_DIR
                + "/PI_Giorgio_Trinchieri/Project_" + PROJECT_ID
                + "/JAMSAlpha/reads/sample.fastq";
        assertEquals(expected, liciCisPathMetadataProcessorImpl.getArchivePath(statusInfo));
    }

    @Test
    public void testGetArchivePath_JAMStarballs() throws Exception {
        StatusInfo statusInfo = setupStatusInfo(
                "/data/Trinchieri_lab/JAMSarchive/JAMSdb202201/JAMStarballs",
                "/data/Trinchieri_lab/JAMSarchive/JAMSdb202201/JAMStarballs/sample.tar",
                "sample.tar");

        String expected = DESTINATION_BASE_DIR
                + "/PI_Giorgio_Trinchieri/Project_" + PROJECT_ID
                + "/JAMSAlpha/JAMStarballs/sample.tar";
        assertEquals(expected, liciCisPathMetadataProcessorImpl.getArchivePath(statusInfo));
    }

    @Test
    public void testGetArchivePath_otherSubcollection() throws Exception {
        StatusInfo statusInfo = setupStatusInfo(
                "/data/Trinchieri_lab/JAMSarchive/JAMSdb202201/annotations",
                "/data/Trinchieri_lab/JAMSarchive/JAMSdb202201/annotations/sample.txt",
                "sample.txt");

        String expected = DESTINATION_BASE_DIR
                + "/PI_Giorgio_Trinchieri/Project_" + PROJECT_ID
                + "/JAMSAlpha/sample.txt";
        assertEquals(expected, liciCisPathMetadataProcessorImpl.getArchivePath(statusInfo));
    }

    @Test
    public void testGetArchivePath_nonJAMSarchive() throws Exception {
        StatusInfo statusInfo = setupStatusInfo(
                "/data/Trinchieri_lab/JAMSBeta/JAMSdb202201/analysis",
                "/data/Trinchieri_lab/JAMSBeta/JAMSdb202201/analysis/results.tsv",
                "results.tsv");

        String expected = DESTINATION_BASE_DIR
                + "/PI_Giorgio_Trinchieri/Project_" + PROJECT_ID
                + "/JAMSBeta/results.tsv";
        assertEquals(expected, liciCisPathMetadataProcessorImpl.getArchivePath(statusInfo));
    }

    // ---------------------------------------------------------------------------
    // getMetaDataJson() tests
    // ---------------------------------------------------------------------------

    @Test
    public void testGetMetaDataJson_reads()
            throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

        StatusInfo statusInfo = setupStatusInfo(
                "/data/Trinchieri_lab/JAMSarchive/JAMSdb202201/reads",
                "/data/Trinchieri_lab/JAMSarchive/JAMSdb202201/reads/sample.fastq",
                "sample.fastq");

        // Pre-load metadataMap (getMetaDataJson does not reload it)
        setMetadataMap(buildMetadataMap());

        HpcDataObjectRegistrationRequestDTO dto =
                liciCisPathMetadataProcessorImpl.getMetaDataJson(statusInfo);

        assertNotNull(dto);
        assertEquals(Boolean.TRUE, dto.getCreateParentCollections());
        assertEquals(Boolean.TRUE, dto.getGenerateUploadRequestURL());

        List<HpcBulkMetadataEntry> entries =
                dto.getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries();
        // PI + Project + Output (JAMSAlpha) + Sub-folder (reads) = 4 entries
        assertEquals(4, entries.size());

        // PI collection
        HpcBulkMetadataEntry piEntry = entries.get(0);
        assertEquals(DESTINATION_BASE_DIR + "/PI_Giorgio_Trinchieri", piEntry.getPath());
        assertMetadataEntry(piEntry, "collection_type", "DataOwner_Lab");
        assertMetadataEntry(piEntry, "data_owner", "Giorgio Trinchieri");
        assertMetadataEntry(piEntry, "data_owner_email", "trinchig@mail.nih.gov");
        assertMetadataEntry(piEntry, "data_owner_affiliation", "NCI");

        // Project collection
        HpcBulkMetadataEntry projectEntry = entries.get(1);
        assertEquals(
                DESTINATION_BASE_DIR + "/PI_Giorgio_Trinchieri/Project_" + PROJECT_ID,
                projectEntry.getPath());
        assertMetadataEntry(projectEntry, "collection_type", "Project");
        assertMetadataEntry(projectEntry, "project_id", PROJECT_ID);
        assertMetadataEntry(projectEntry, "project_title", "Test Project Title");
        assertMetadataEntry(projectEntry, "project_status", "Active");
        assertMetadataEntry(projectEntry, "access", "Controlled Access");
        assertMetadataEntry(projectEntry, "retention_years", "7");

        // Output collection (JAMSAlpha)
        HpcBulkMetadataEntry outputEntry = entries.get(2);
        assertEquals(
                DESTINATION_BASE_DIR + "/PI_Giorgio_Trinchieri/Project_" + PROJECT_ID
                        + "/JAMSAlpha",
                outputEntry.getPath());
        assertMetadataEntry(outputEntry, "collection_type", "Output");

        // Sub-folder (reads)
        HpcBulkMetadataEntry subFolderEntry = entries.get(3);
        assertEquals(
                DESTINATION_BASE_DIR + "/PI_Giorgio_Trinchieri/Project_" + PROJECT_ID
                        + "/JAMSAlpha/reads",
                subFolderEntry.getPath());
        assertMetadataEntry(subFolderEntry, "collection_type", "Processed_Data");

        // Object metadata
        List<HpcMetadataEntry> objectMetadata = dto.getMetadataEntries();
        assertEquals(3, objectMetadata.size());
        assertEquals("object_name", objectMetadata.get(0).getAttribute());
        assertEquals("sample.fastq", objectMetadata.get(0).getValue());
        assertEquals("source_path", objectMetadata.get(1).getAttribute());
        assertEquals(
                "/data/Trinchieri_lab/JAMSarchive/JAMSdb202201/reads",
                objectMetadata.get(1).getValue());
        assertEquals("file_type", objectMetadata.get(2).getAttribute());
        assertEquals("fastq", objectMetadata.get(2).getValue());
    }

    @Test
    public void testGetMetaDataJson_nonJAMSarchive()
            throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

        StatusInfo statusInfo = setupStatusInfo(
                "/data/Trinchieri_lab/JAMSBeta/JAMSdb202201/analysis",
                "/data/Trinchieri_lab/JAMSBeta/JAMSdb202201/analysis/results.tsv",
                "results.tsv");

        setMetadataMap(buildMetadataMap());

        HpcDataObjectRegistrationRequestDTO dto =
                liciCisPathMetadataProcessorImpl.getMetaDataJson(statusInfo);

        assertNotNull(dto);
        assertEquals(Boolean.TRUE, dto.getCreateParentCollections());
        assertEquals(Boolean.TRUE, dto.getGenerateUploadRequestURL());

        List<HpcBulkMetadataEntry> entries =
                dto.getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries();
        // PI + Project + Analysis (JAMSBeta) = 3 entries
        assertEquals(3, entries.size());

        // PI collection
        HpcBulkMetadataEntry piEntry = entries.get(0);
        assertEquals(DESTINATION_BASE_DIR + "/PI_Giorgio_Trinchieri", piEntry.getPath());
        assertMetadataEntry(piEntry, "collection_type", "DataOwner_Lab");

        // Project collection
        HpcBulkMetadataEntry projectEntry = entries.get(1);
        assertEquals(
                DESTINATION_BASE_DIR + "/PI_Giorgio_Trinchieri/Project_" + PROJECT_ID,
                projectEntry.getPath());
        assertMetadataEntry(projectEntry, "collection_type", "Project");

        // Analysis (JAMSBeta) collection
        HpcBulkMetadataEntry analysisEntry = entries.get(2);
        assertEquals(
                DESTINATION_BASE_DIR + "/PI_Giorgio_Trinchieri/Project_" + PROJECT_ID
                        + "/JAMSBeta",
                analysisEntry.getPath());
        assertMetadataEntry(analysisEntry, "collection_type", "Analysis");

        // Object metadata
        List<HpcMetadataEntry> objectMetadata = dto.getMetadataEntries();
        assertEquals(3, objectMetadata.size());
        assertEquals("object_name", objectMetadata.get(0).getAttribute());
        assertEquals("results.tsv", objectMetadata.get(0).getValue());
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private StatusInfo setupStatusInfo(String originalFilePath, String sourceFilePath,
            String sourceFileName) {
        StatusInfo statusInfo = new StatusInfo();
        statusInfo.setOriginalFilePath(originalFilePath);
        statusInfo.setSourceFilePath(sourceFilePath);
        statusInfo.setSourceFileName(sourceFileName);
        return statusInfo;
    }

    /**
     * Builds a metadata map that covers both JAMSarchive and JAMSBeta test keys with all
     * required fields.
     */
    private Map<String, Map<String, String>> buildMetadataMap() {
        Map<String, String> rowData = new HashMap<>();
        rowData.put("project_id", PROJECT_ID);
        rowData.put("data_owner", "Giorgio Trinchieri");
        rowData.put("data_owner_email", "trinchig@mail.nih.gov");
        rowData.put("data_owner_affiliation", "NCI");
        rowData.put("data_generator", "Data Generator");
        rowData.put("data_generator_affiliation", "NCI");
        rowData.put("data_generator_email", "generator@mail.nih.gov");
        rowData.put("project_poc", "POC Name");
        rowData.put("project_poc_affiliation", "NCI");
        rowData.put("project_poc_email", "poc@mail.nih.gov");
        rowData.put("project_title", "Test Project Title");
        rowData.put("project_start_date", "01/01/22");
        rowData.put("project_description", "Test project description");
        rowData.put("platform_name", "Illumina");
        rowData.put("organism", "Mus musculus");
        rowData.put("study_disease", "Cancer");
        rowData.put("data_generating_facility", "NCI LICI");

        Map<String, Map<String, String>> metadataMap = new HashMap<>();
        metadataMap.put(METADATA_KEY_JAMSARCHIVE, rowData);
        metadataMap.put(METADATA_KEY_JAMSBETA, rowData);
        return metadataMap;
    }

    /** Sets the inherited {@code metadataMap} field directly on the processor. */
    private void setMetadataMap(Map<String, Map<String, String>> map) throws Exception {
        Field field = AbstractPathMetadataProcessor.class.getDeclaredField("metadataMap");
        field.setAccessible(true);
        field.set(liciCisPathMetadataProcessorImpl, map);
    }

    /** Asserts that a bulk metadata entry contains an entry with the given attribute and value. */
    private void assertMetadataEntry(HpcBulkMetadataEntry entry, String attribute, String value) {
        for (HpcMetadataEntry metadataEntry : entry.getPathMetadataEntries()) {
            if (attribute.equals(metadataEntry.getAttribute())) {
                assertEquals("Mismatch for attribute '" + attribute + "'",
                        value, metadataEntry.getValue());
                return;
            }
        }
        throw new AssertionError(
                "Metadata entry with attribute '" + attribute + "' not found in path: "
                        + entry.getPath());
    }
}

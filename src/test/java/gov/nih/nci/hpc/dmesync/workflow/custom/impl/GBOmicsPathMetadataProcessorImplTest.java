package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import org.junit.jupiter.api.Test;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;

import static org.junit.jupiter.api.Assertions.*;

class GBOmicsPathMetadataProcessorImplTest {

    @Test
    void testGetProjectCollectionNameCCRGB() throws Exception {
    	
        GBOmicsPathMetadataProcessorImpl processor = new GBOmicsPathMetadataProcessorImpl();
        
        StatusInfo statusInfo = new StatusInfo();
        
        // Simulate a path where the project folder is named "ccrgb-13"
        statusInfo.setOriginalFilePath("/data/khanlab3/gb_omics/projects/caplen/ccrgb-13/file.txt");
        statusInfo.setSourceFilePath("/data/khanlab3/gb_omics/projects/caplen/ccrgb-13/file.txt");
        
        // If needed, set up processor fields (e.g., sourceBaseDir, createCollectionSoftlink)
        java.lang.reflect.Field sourceBaseDirField = GBOmicsPathMetadataProcessorImpl.class.getDeclaredField("sourceBaseDir");
        sourceBaseDirField.setAccessible(true);
        sourceBaseDirField.set(processor, "/data/khanlab3/gb_omics/projects");
        java.lang.reflect.Field createCollectionSoftlinkField = GBOmicsPathMetadataProcessorImpl.class.getDeclaredField("createCollectionSoftlink");
        createCollectionSoftlinkField.setAccessible(true);
        createCollectionSoftlinkField.set(processor, false);
        
        // The PI folder is "caplen" in this path
        String result = processor.getProjectCollectionName(statusInfo);
        assertEquals("CCRGB-13", result);
    }
    
    @Test
    void testGetProjectCollectionNameOther() throws Exception {
    	
        GBOmicsPathMetadataProcessorImpl processor = new GBOmicsPathMetadataProcessorImpl();
        
        StatusInfo statusInfo = new StatusInfo();
        
        // Simulate a path where the project folder is named "ccrgb-13"
        statusInfo.setOriginalFilePath("/data/khanlab3/gb_omics/projects/caplen/CENPA_cutrun/file.txt");
        statusInfo.setSourceFilePath("/data/khanlab3/gb_omics/projects/caplen/CENPA_cutrun/file.txt");
        
        // If needed, set up processor fields (e.g., sourceBaseDir, createCollectionSoftlink)
        java.lang.reflect.Field sourceBaseDirField = GBOmicsPathMetadataProcessorImpl.class.getDeclaredField("sourceBaseDir");
        sourceBaseDirField.setAccessible(true);
        sourceBaseDirField.set(processor, "/data/khanlab3/gb_omics/projects");
        java.lang.reflect.Field createCollectionSoftlinkField = GBOmicsPathMetadataProcessorImpl.class.getDeclaredField("createCollectionSoftlink");
        createCollectionSoftlinkField.setAccessible(true);
        createCollectionSoftlinkField.set(processor, false);
        
        // The PI folder is "caplen" in this path
        String result = processor.getProjectCollectionName(statusInfo);
        assertEquals("CENPA_cutrun", result);
    }
}
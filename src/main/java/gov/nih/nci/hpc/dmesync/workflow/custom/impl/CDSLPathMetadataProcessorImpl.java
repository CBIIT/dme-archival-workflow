package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 *
 * @author Guillermo Suchicital
 */


@Service("cdsl")
public class CDSLPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.source.base.dir}")
	private String sourceDir;

	private String sampleDir;
	
	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("-  --  CDSL custom DmeSyncPathMetadataProcessor start process for object {}", object.getId());
		
		String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
		
		String elPath = object.getOriginalFilePath();	
		String archivePath;
		String archivePath_Raw_Data ;
		String archivePath_Analysis ;
		
		archivePath_Raw_Data = destinationBaseDir + "/" +  getGivenDirectory_PI(elPath, "DataOwner") + "/" + getGivenDirectory(elPath, "Project") + "/"  +  getGivenDirectory(elPath, "Sample") + "/"  +  getGivenDirectory(elPath, "Run") + "/"  +"fast5/"              + fileName;
		archivePath_Analysis = destinationBaseDir + "/" +  getGivenDirectory_PI(elPath, "DataOwner") +  "/"+ getGivenDirectory(elPath, "Project") + "/"  +  getGivenDirectory(elPath, "Sample")+ "/"   +  getGivenDirectory(elPath, "Run") + "/" + "Sequencing_Reports/" + fileName;
		
		//------------ is it Analysis or Raw_Data ?  ---------------
		
	
		if (object.getSourceFilePath().contains("fast5") ) {	
		
			    archivePath = archivePath_Raw_Data ;
		}
		else {
			    archivePath = archivePath_Analysis; 			    
	    }          
	

	    logger.info("CDSL Archive path for {} : {}", object.getOriginalFilePath(), archivePath);
	
		return archivePath;				
	}
	
	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {
		
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();	
	
		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// ------  SET UP DIRECTORIES FOR EACH SECTION  -------
		
		String elPath = object.getOriginalFilePath();
		
		//owner
		
		String piCollectionPath =  destinationBaseDir + "/"  +  getGivenDirectory_PI(elPath, "DataOwner");	
		//project
	
		String projectCollectionPath = piCollectionPath + "/" + getGivenDirectory(elPath, "Project");	
	
		//sample
		String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
		String sampleCollectionPath;		
		sampleCollectionPath = projectCollectionPath     + "/" + getGivenDirectory(elPath, "Sample");
		
		//run
		String runCollectionPath;
		runCollectionPath =   sampleCollectionPath  +"/"   +  getGivenDirectory(elPath, "Run") ;	
		
		
		//raw
		String rawDATA__CollectionPath;
		rawDATA__CollectionPath =  runCollectionPath  +"/"  + "fast5";
		
		//analysis		
		String analysisCollectionPath;
		analysisCollectionPath =   runCollectionPath  +"/" + "Sequencing_Reports" ;
		
		
		// ----------------    Add path metadata entries for <  DataOwner_Lab  > collection    --------------------------		
		String metadata_ownerFile = "/Users/suchicitalgm/data/KolmogorovLab/owner_metadata.xltm";
		
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));				
		
		pathEntriesPI.setPath(piCollectionPath);
			
		threadLocalMap.set(loadMetadataFile(metadata_ownerFile, "data_owner"));  
		
		String key1 =  getGivenDirectory(metadata_ownerFile, "Lab") ;
		
		
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner",   key1         ));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_email",       getAttrValueWithKey(key1, "data_owner_email")));	
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation", getAttrValueWithKey(key1, "data_owner_affiliation")));     
	
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);
		
		
		//---------------------      Add path metadata entries for <  PROJECT  > collection    ----------------------------------------------------------
		String metadata_ProjectFile = "/Users/suchicitalgm/data/KolmogorovLab/ArchivalData/DataOwner_Lab_Kolmogorov/project_metadata.xltx";
		
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));					
		pathEntriesProject.setPath(projectCollectionPath);
		
		threadLocalMap.set(loadMetadataFile(metadata_ProjectFile, "project_id"));  
			
		String key2 =  getGivenDirectory(elPath, "Project") ;	
		
		pathEntriesProject.getPathMetadataEntries().add( createPathEntry("project_id",               key2  ));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title",             getAttrValueWith_Key( key2 , "project_title")));			
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description",       getAttrValueWith_Key( key2 , "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date",        getAttrValueWith_Key( key2 , "project_start_date")));		
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism",                  getAttrValueWith_Key( key2 , "organism")));		
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("is_cell_line",              getAttrValueWith_Key( key2 , "is_cell_line")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("study_disease",             getAttrValueWith_Key( key2 , "study_disease")));			
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc",               getAttrValueWith_Key( key2 , "project_poc"))); 
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation",   getAttrValueWith_Key( key2 , "project_poc_affiliation"))); 
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email",         getAttrValueWith_Key( key2 , "project_poc_email")));		
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date",    getAttrValueWith_Key( key2 , "project_completed_date")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status",            getAttrValueWith_Key( key2 , "project_status")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("retention_years",           getAttrValueWith_Key( key2 , "retention_years")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access",                    getAttrValueWith_Key( key2 , "access")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility",  getAttrValueWith_Key( key2 , "data_generating_facility")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator",          getAttrValueWith_Key( key2 , "key_collaborator")));
		
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		
		// ---------------       Add path metadata entries for    <  SAMPLE  >    collection section     -----------								
		String metadata_SampleFile = "/Users/suchicitalgm/data/KolmogorovLab/ArchivalData/DataOwner_Lab_Kolmogorov/Project_MouseMelanoma2905/sample_metadata.xltx";
	 
		HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();		
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));		
	    pathEntriesSample.setPath(sampleCollectionPath);
		
		threadLocalMap.set(loadMetadataFile(metadata_SampleFile, "sample_id"));  
		
		String key3 =  getGivenDirectory(elPath, "Sample") ;
		
		String partKey[]= key3.split("_");
		key3 = partKey[1];
	
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", key3));
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_strategy", getAttrValueWith_Key(key3, "library_strategy")));	
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry("analyte_type",     getAttrValueWith_Key(key3, "analyte_type")));	
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue_type",      getAttrValueWith_Key(key3, "tissue_type")));	
										
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);			
		
		
		// ---------------       Add path metadata entries for     <    RUN    >      collection section     -----------	
		
        HpcBulkMetadataEntry path____ENTRIES___RUN = new HpcBulkMetadataEntry();	             
        path____ENTRIES___RUN.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));      			
        path____ENTRIES___RUN.setPath(runCollectionPath);
		
        path____ENTRIES___RUN.getPathMetadataEntries().add(createPathEntry("run_id",   getGivenDirectory(elPath, "Run"))  ) ;	
		
		
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(path____ENTRIES___RUN);	
			
		
		//----------------     LOGIC FOR ALTERNATE ENTRIES PATHS  < RAW_DATA >  OR < ANALYSIS >   ---------
		if (object.getSourceFilePath().contains("fast5")) {	
									
				HpcBulkMetadataEntry pathEntries____RAW_DATA = new HpcBulkMetadataEntry();	
								
				pathEntries____RAW_DATA.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "fast5")); 
				pathEntries____RAW_DATA.setPath(rawDATA__CollectionPath); 
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntries____RAW_DATA);
		}
		else {			   
				HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();	
				
				pathEntriesAnalysis.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sequencing_Reports"));      			
				pathEntriesAnalysis.setPath(analysisCollectionPath);
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);	
		}	
					
		//-------------------------------------     PREPARE  JASON  and Return METADATA    -----------------------------------------------------------	
		// Set it to dataObjectRegistrationRequestDTO		
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);    
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);	
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries); 
	
		// Add object metadata
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("source_path", object.getOriginalFilePath()));
		
		logger.info("-  --  CDSL custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		
		return dataObjectRegistrationRequestDTO;
	
	}
		

	
		
	public String getSampleDir() {
		return sampleDir;
	}

	public void setSampleDir(String sampleDir) {
		this.sampleDir = sampleDir;
	}
	
	
	private String getGivenDirectory_PI(String elPath, String match) {
		
	    List<String> list = new ArrayList <String> (Arrays.asList(elPath.split("/")));
		String sampleDir ="";
			
		for(String x : list) {
			
				
			if (x.toUpperCase().contains(match.toUpperCase())) {
					sampleDir = x;
		
			}
		
		}
			
	    String []list2  =  sampleDir.split("_");
		String lastStr = "PI_" + "Mikhail"+ "_"  + list2[2] ;
			
		return lastStr;		
	}
	
	
  public String getAttrValueWith_Key(String rowKey, String attrKey) {
		String key = null;
		if(threadLocalMap.get() == null)
		  return null;
		for (String partialKey : threadLocalMap.get().keySet()) {
			if (StringUtils.contains(rowKey, partialKey)) {
		      key = partialKey;
		      break;
		    }
	    }
		if(StringUtils.isEmpty(key)) {
	      logger.error("Excel mapping not found for {}", rowKey);
	      return null;
	    }
		
		return (threadLocalMap.get().get(key) == null? null : threadLocalMap.get().get(key).get(attrKey));
	  }
	  
	  public String getAttrValueWithExact_Key(String key, String attrKey) {
		if(StringUtils.isEmpty(key)) {
	      logger.error("Excel mapping not found for {}", key);
	      return null;
	    }
	    return (threadLocalMap.get().get(key) == null? null : threadLocalMap.get().get(key).get(attrKey));
	  }
		  
	
   private String getGivenDirectory(String elPath, String match) {
		
	    List<String> list = new ArrayList <String> (Arrays.asList(elPath.split("/")));
		String sampleDir ="";
			
		for(String x : list) {
			
				
			if (x.toUpperCase().contains(match.toUpperCase())) {
					sampleDir = x;
			}
		}
				
		
		return sampleDir;		
	}
		

}

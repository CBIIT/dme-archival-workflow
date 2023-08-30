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
	private String archivePath;
	private String sampleDir;
	
	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("-  --  CDSL custom DmeSyncPathMetadataProcessor start process for object {}", object.getId());
		
		String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
		String archivePath = null;
		String archivePath_1 = null;
		String archivePath_2 = null;
     
		
		String elPath = object.getOriginalFilePath();
		String sampleDir = "";
		
		sampleDir = getSampleDirectory(elPath);
		this.setSampleDir(sampleDir);
		
		
		archivePath_1 = destinationBaseDir + "/PI_"  +  getPiCollectionName(object) + "/Project_"  +  getProjectCollectionName(object) + "/"  +  sampleDir + "/"  + "Run_X" + "/"  +"Raw_Data/" + fileName;
		archivePath_2 = destinationBaseDir + "/PI_"  +  getPiCollectionName(object) + "/Project_"  +  getProjectCollectionName(object) + "/"  +  sampleDir + "/"  + "Run_X" + "/" + "Analysis/" + fileName;
		
		//------------ is it Analysis or Raw_Data ?  ---------------
		
		if (object.getSourceFilePath().contains("fast5") ) {	
		
			    archivePath = archivePath_1;
		}
		else {
			    archivePath = archivePath_2; 			    
	    }          
	
		this.archivePath= archivePath;
		
		
	    logger.info("CDSL Archive path for {} : {}", object.getOriginalFilePath(), archivePath);
	
		return archivePath;				
	}
	
	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {
		
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();	
	
		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// ------  SET UP DIRECTORIES FOR EACH SECTION  -------
		
		//owner
		String piCollectionName = getPiCollectionName(object);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;	
		//project
		String projectCollectionName = getProjectCollectionName(object);
		String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;	
		//sample
		String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
		String sampleId = getSampleId(object);
		String sampleCollectionPath;		
		sampleCollectionPath = projectCollectionPath     + "/" + this.getSampleDir();
		//run
		String runCollectionPath;
		runCollectionPath =   sampleCollectionPath  +"/" + getRunName(object) + "_X";	
		//raw
		String rawDATA__CollectionPath;
		rawDATA__CollectionPath =  runCollectionPath  +"/"  + "Raw_Data";
		//analysis		
		String analysisCollectionPath;
		analysisCollectionPath =   runCollectionPath  +"/" + getAnalysisName(object) ;
				
		
		// ----------------    Add path metadata entries for <  DataOwner_Lab  > collection    --------------------------		
		String metadata_ownerFile = "/Users/suchicitalgm/data/KolmogorovLab/owner_metadata.xltm";
		
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));				
		
		pathEntriesPI.setPath(piCollectionPath);
			
		threadLocalMap.set(loadMetadataFile(metadata_ownerFile, "data_owner"));  
		
		String origenPiCollectionName = getOrigenPiCollectionName(object);
	
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner",              "KolmogorovLab" ));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_email",       getAttrValueWithKey(origenPiCollectionName, "data_owner_email")));	
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation", getAttrValueWithKey(origenPiCollectionName, "data_owner_affiliation")));     
	
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);
		
		//---------------------      Add path metadata entries for <  PROJECT  > collection    ----------------------------------------------------------
		String metadata_ProjectFile = "/Users/suchicitalgm/data/KolmogorovLab/ArchivalData/DataOwner_Lab_Kolmogorov/project_metadata.xltx";
		
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));					
		pathEntriesProject.setPath(projectCollectionPath);
		
		threadLocalMap.set(loadMetadataFile(metadata_ProjectFile, "project_id"));  
			
		pathEntriesProject.getPathMetadataEntries().add( createPathEntry("project_id",               getProjectCollectionNameSmall(object)  ));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title",             getAttrValueWithKey( getProjectCollectionNameSmall(object) , "project_title")));			
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description",       getAttrValueWithKey( getProjectCollectionNameSmall(object) , "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date",        getAttrValueWithKey( getProjectCollectionNameSmall(object) , "project_start_date")));		
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism",                  getAttrValueWithKey( getProjectCollectionNameSmall(object) , "organism")));		
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("is_cell_line",              getAttrValueWithKey( getProjectCollectionNameSmall(object) , "is_cell_line")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("study_disease",             getAttrValueWithKey( getProjectCollectionNameSmall(object) , "study_disease")));			
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc",               getAttrValueWithKey( getProjectCollectionNameSmall(object) , "project_poc"))); 
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation",   getAttrValueWithKey( getProjectCollectionNameSmall(object) , "project_poc_affiliation"))); 
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email",         getAttrValueWithKey( getProjectCollectionNameSmall(object) , "project_poc_email")));		
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date",    getAttrValueWithKey( getProjectCollectionNameSmall(object) , "project_completed_date")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status",            getAttrValueWithKey( getProjectCollectionNameSmall(object) , "project_status")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("retention_years",           getAttrValueWithKey( getProjectCollectionNameSmall(object) , "retention_years")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access",                    getAttrValueWithKey( getProjectCollectionNameSmall(object) , "access")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility",  getAttrValueWithKey( getProjectCollectionNameSmall(object) , "data_generating_facility")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator",          getAttrValueWithKey( getProjectCollectionNameSmall(object) , "key_collaborator")));
		
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		// ---------------       Add path metadata entries for    <  SAMPLE  >    collection section     -----------								
		String metadata_SampleFile = "/Users/suchicitalgm/data/KolmogorovLab/ArchivalData/DataOwner_Lab_Kolmogorov/Project_MouseMelanoma2905/sample_metadata.xltx";
	 
		HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();		
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));		
	    pathEntriesSample.setPath(sampleCollectionPath);
		
		threadLocalMap.set(loadMetadataFile(metadata_SampleFile, "sample_id"));  
		
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", "C1"));
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_strategy", getAttrValueWithKey("C1", "library_strategy")));	
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry("analyte_type",     getAttrValueWithKey("C1", "analyte_type")));	
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue_type",      getAttrValueWithKey("C1", "tissue_type")));	
										
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);			
			
		// ---------------       Add path metadata entries for     <    RUN    >      collection section     -----------	
		String metadata_RunFile = "/Users/suchicitalgm/data/KolmogorovLab/ArchivalData/DataOwner_Lab_Kolmogorov/Project_MouseMelanoma2905/run_metadata.xltx";
		
        HpcBulkMetadataEntry path____ENTRIES___RUN = new HpcBulkMetadataEntry();	             
        path____ENTRIES___RUN.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));      			
        path____ENTRIES___RUN.setPath(runCollectionPath);
		
		threadLocalMap.set(loadMetadataFile(metadata_RunFile,"run_id" ));  
		
		path____ENTRIES___RUN.getPathMetadataEntries().add(createPathEntry("run_id",   "RUN_X" ));	
				
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(path____ENTRIES___RUN);	
					
		//----------------     LOGIC FOR ALTERNATE ENTRIES PATHS  < RAW_DATA >  OR < ANALYSIS >   ---------
		if (object.getSourceFilePath().contains("fast5")) {	
									
				HpcBulkMetadataEntry pathEntries____RAW_DATA = new HpcBulkMetadataEntry();	
								
				pathEntries____RAW_DATA.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data")); 
				pathEntries____RAW_DATA.setPath(rawDATA__CollectionPath); 
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntries____RAW_DATA);
		}
		else {			   
				HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();	
				
				pathEntriesAnalysis.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));      			
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
		
	
	public String getArchivePath() {
		return archivePath;
	}

	public void setArchivePath(String archivePath) {
		this.archivePath = archivePath;
	}
	

	private String getCollectionNameFromParent(StatusInfo object, String parentName) {
		Path fullFilePath = Paths.get(object.getOriginalFilePath());
		logger.info("Full File Path = {}", fullFilePath);
		int count = fullFilePath.getNameCount();
		for (int i = 0; i <= count; i++) {
			if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
				return fullFilePath.getFileName().toString();
			}
			fullFilePath = fullFilePath.getParent();
		}
		return null;
	}	
	
	private String getSampleId(StatusInfo object) throws DmeSyncMappingException {
		String fileName = Paths.get(object.getOriginalFilePath()).getFileName().toString();
		String sampleId = null;
		sampleId = StringUtils.substringBefore(fileName, "_");

		return sampleId;
	}
	
	private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String piCollectionName = null;
		
		piCollectionName = "Mikhail_Kolmogorov";	
		return piCollectionName;
	}
		
	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		
		String projectCollectionName = "MouseMelanoma2905";		
		return projectCollectionName;
	}
		
	
	private String getProjectCollectionNameSmall(StatusInfo object) throws DmeSyncMappingException {
		String projectCollectionName = null;
		//String parentName = Paths.get(sourceDir).getFileName().toString();
		projectCollectionName = "MouseMelanoma2905";
		//logger.info("projectCollectionName: {}", projectCollectionName);
		return projectCollectionName;
	}
	
	
	private String getAnalysisName(StatusInfo object) {
		//String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		return "Analysis";
		
	}
	
	private String getSampleName(StatusInfo object) {
	    String sample = "Sample";
		
	    return sample;	
	}
	
	private String getRunName(StatusInfo object) {
		String sample = "Run";
			
		return sample;	
	}
	
	private String getOrigenPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String piCollectionName = null;
		piCollectionName = "KolmogorovLab";
		
		return piCollectionName;
	}
		
	public String getSampleDir() {
		return sampleDir;
	}

	public void setSampleDir(String sampleDir) {
		this.sampleDir = sampleDir;
	}
	
	
	private String getSampleDirectory(String elPath) {
		
	    List<String> list = new ArrayList <String> (Arrays.asList(elPath.split("/")));
		String sampleDir ="";
			
		for(String x : list) {
				
			if (x.toUpperCase().contains("Sample".toUpperCase())) {
					sampleDir = x;
			}
		}
				
		return sampleDir;		
	}
	
	
	private String getProjectCollection_Name(StatusInfo object) {
		String parent = Paths.get(object.getSourceFilePath()).getParent().getFileName().toString();
		String project = null;
		if (parent.equals("bam")) {
			project = "Data_BAM";
		} else if (parent.equals("fastq")) {
			project = "Data_FASTQ";
		} else if (parent.equals("sequence")) {
			project = "Data_BCL";
		}
		return project;
	}
	
	private String getProjectCollectionNameRecursive(StatusInfo object) throws DmeSyncMappingException {
		String projectCollectionName = null;
		String parentName = Paths.get(sourceDir).getFileName().toString();
		projectCollectionName = getCollectionNameFromParent(object, getCollectionNameFromParent(object, parentName));

		logger.info("projectCollectionName: {}", projectCollectionName);
		return projectCollectionName;
	}

	

}

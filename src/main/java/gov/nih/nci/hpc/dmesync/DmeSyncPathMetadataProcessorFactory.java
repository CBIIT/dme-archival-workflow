package gov.nih.nci.hpc.dmesync;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;

@Component
public class DmeSyncPathMetadataProcessorFactory {
  @Autowired
  @Qualifier("default")
  private DmeSyncPathMetadataProcessor defaultProcessor;

  @Autowired
  @Qualifier("hitif")
  private DmeSyncPathMetadataProcessor hitifProcessor;

  @Autowired
  @Qualifier("cmm")
  private DmeSyncPathMetadataProcessor cmmProcessor;
  
  @Autowired
  @Qualifier("ncef")
  private DmeSyncPathMetadataProcessor ncefProcessor;
  
  @Autowired
  @Qualifier("ncep")
  private DmeSyncPathMetadataProcessor ncepProcessor;
  
  @Autowired
  @Qualifier("seer")
  private DmeSyncPathMetadataProcessor seerProcessor;
  
  @Autowired
  @Qualifier("sb")
  private DmeSyncPathMetadataProcessor sbProcessor;

  @Autowired
  @Qualifier("lcb")
  private DmeSyncPathMetadataProcessor lcbProcessor;
  
  @Autowired
  @Qualifier("compass")
  private DmeSyncPathMetadataProcessor compassProcessor;
  
  @Autowired
  @Qualifier("lrbge")
  private DmeSyncPathMetadataProcessor lrbgeProcessor;
  
  @Autowired
  @Qualifier("uob")
  private DmeSyncPathMetadataProcessor uobProcessor;
  
  @Autowired
  @Qualifier("nice")
  private DmeSyncPathMetadataProcessor niceProcessor;
  
  @Autowired
  @Qualifier("dbgap")
  private DmeSyncPathMetadataProcessor dbgapProcessor;
  
  @Autowired
  @Qualifier("ega")
  private DmeSyncPathMetadataProcessor egaProcessor;
  
  @Autowired
  @Qualifier("biobank")
  private DmeSyncPathMetadataProcessor biobankProcessor;
  
  @Autowired
  @Qualifier("dceg")
  private DmeSyncPathMetadataProcessor dcegProcessor;
  
  @Autowired
  @Qualifier("csb")
  private DmeSyncPathMetadataProcessor csbProcessor;
  
  @Autowired
  @Qualifier("template")
  private DmeSyncPathMetadataProcessor templateProcessor;
  
  @Autowired
  @Qualifier("mocha")
  private DmeSyncPathMetadataProcessor mochaProcessor;
  
  @Autowired
  @Qualifier("scaf")
  private DmeSyncPathMetadataProcessor scafProcessor;
  
  @Autowired
  @Qualifier("lcp")
  private DmeSyncPathMetadataProcessor lcpProcessor;
  
  public DmeSyncPathMetadataProcessor getService(String doc) {
    if ("hitif".equals(doc)) {
      return hitifProcessor;
    } else if ("cmm".equals(doc)) {
      return cmmProcessor;
    } else if("ncef".equals(doc)) {
    	return ncefProcessor;
    }  else if("ncep".equals(doc)) {
        return ncepProcessor;
    } else if ("seer".equals(doc)){
    	return seerProcessor;
    } else if ("sb".equals(doc)){
    	return sbProcessor;
    } else if ("lcb".equals(doc)){
        return lcbProcessor;
    } else if ("compass".equals(doc)){
        return compassProcessor;
    } else if ("lrbge".equals(doc)){
        return lrbgeProcessor;
    } else if ("uob".equals(doc)){
        return uobProcessor;
    } else if ("nice".equals(doc)){
        return niceProcessor;
    } else if ("dbgap".equals(doc)){
      return dbgapProcessor;
    } else if ("ega".equals(doc)){
      return egaProcessor;
    } else if ("biobank".equals(doc)){
        return biobankProcessor;
    } else if ("dceg".equals(doc)){
        return dcegProcessor;
    } else if ("csb".equals(doc)){
        return csbProcessor;
    }  else if ("template".equals(doc)){
        return templateProcessor;
    }  else if ("mocha".equals(doc)){
        return mochaProcessor;
    }  else if ("scaf".equals(doc)){
        return scafProcessor;
    }  else if ("lcp".equals(doc)){
        return lcpProcessor;
    } else {
        return defaultProcessor;
    }
  }
}

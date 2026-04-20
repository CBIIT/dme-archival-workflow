package gov.nih.nci.hpc.dmesync.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.dto.validation.FrameworkDocRulesResponse;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;

/**
 * Service to fetch framework metadata validation rules from DME API
 * 
 * @author GitHub Copilot
 */
@Service
public class FrameworkMetadataRulesService {
  
  protected Logger logger = LoggerFactory.getLogger(this.getClass());
  
  @Autowired
  private RestTemplateFactory restTemplateFactory;
  
  @Autowired
  private ObjectMapper objectMapper;
  
  @Value("${hpc.server.url}")
  private String serverUrl;
  
  @Value("${auth.token}")
  private String authToken;
  
  @Value("${dmesync.framework.rules.path:/metadata/frameworkRules}")
  private String rulesPath;
  
  /**
   * Fetch validation rules from DME API with caching
   * 
   * @param doc Document/DOC name
   * @param userContext Optional user context for cache key differentiation
   * @return FrameworkDocRulesResponse containing validation rules
   * @throws DmeSyncWorkflowException if API call fails or response cannot be parsed
   */
  @Cacheable(value = "frameworkRules", key = "#doc + '-' + (#userContext ?: 'default')", sync = true)
  public FrameworkDocRulesResponse getRules(String doc, String userContext) 
      throws DmeSyncWorkflowException {
    
    logger.info("Fetching framework metadata validation rules for doc: {} from DME API", doc);
    
    try {
      RestTemplate restTemplate = restTemplateFactory.getRestTemplate();
      
      // Build the URL
      String url = serverUrl + rulesPath;
      
      // Setup headers with authorization
      HttpHeaders headers = new HttpHeaders();
      headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + authToken);
      
      HttpEntity<String> entity = new HttpEntity<>(headers);
      
      // Make the API call
      ResponseEntity<String> response = restTemplate.exchange(
          url,
          HttpMethod.GET,
          entity,
          String.class
      );
      
      // Parse the response
      if (response.getBody() == null) {
        throw new DmeSyncWorkflowException("Empty response from DME framework rules API");
      }
      
      FrameworkDocRulesResponse rulesResponse = objectMapper.readValue(
          response.getBody(), 
          FrameworkDocRulesResponse.class
      );
      
      logger.info("Successfully fetched and cached framework rules for doc: {}", doc);
      return rulesResponse;
      
    } catch (Exception e) {
      logger.error("Failed to fetch framework metadata validation rules from DME API", e);
      throw new DmeSyncWorkflowException(
          "Failed to fetch framework metadata validation rules from DME API: " + e.getMessage(), 
          e
      );
    }
  }
  
  /**
   * Fetch validation rules from DME API with caching (without user context)
   * 
   * @param doc Document/DOC name
   * @return FrameworkDocRulesResponse containing validation rules
   * @throws DmeSyncWorkflowException if API call fails or response cannot be parsed
   */
  public FrameworkDocRulesResponse getRules(String doc) throws DmeSyncWorkflowException {
    return getRules(doc, null);
  }
}

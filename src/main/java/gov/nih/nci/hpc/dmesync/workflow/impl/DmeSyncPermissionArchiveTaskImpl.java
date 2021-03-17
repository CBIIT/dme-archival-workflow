package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.IOException;
import java.net.URI;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.RestTemplateResponseErrorHandler;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
import gov.nih.nci.hpc.dto.datamanagement.HpcArchivePermissionsRequestDTO;
import gov.nih.nci.hpc.dto.error.HpcExceptionDTO;

/**
 * DME Sync Permission Archive Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncPermissionArchiveTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

	@Value("${hpc.server.url}")
	private String serverUrl;

	@Value("${auth.token}")
	private String authToken;

	@Autowired
	private RestTemplateFactory restTemplateFactory;
	@Autowired
	private ObjectMapper objectMapper;

	@PostConstruct
	public boolean init() {
		super.setTaskName("PermissionArchiveTask");
		return true;
	}

	@Override
	public StatusInfo process(StatusInfo object) {

		if (object.getArchivePermissionsRequestDTO() != null) {
			try {
				// Add a delay for this task since File System Upload is async.
				Thread.sleep(120000L);

				// Get owner, group and permissions from the
				// ArchivePermissionsRequestDTO
				setPermissions(object);
			} catch (Exception e) {
				logger.error("[{}] Error occured during set archive permission task", super.getTaskName(), e);
			}
		}
		return object;
	}

	private boolean setPermissions(StatusInfo object) {
		// Call set archive permission API
		HpcArchivePermissionsRequestDTO dto = object.getArchivePermissionsRequestDTO();
		dto.setSetArchivePermissionsFromSource(false);
		dto.setSetDataManagementPermissions(true);

		final URI permissionArchiveUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/dataObject/{path}/acl/archive").buildAndExpand(object.getFullDestinationPath()).encode()
				.toUri();

		HttpHeaders header = new HttpHeaders();
		header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

		ResponseEntity<Object> response = restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler())
				.exchange(permissionArchiveUrl, HttpMethod.POST, new HttpEntity<Object>(dto, header), Object.class);

		if (HttpStatus.OK.equals(response.getStatusCode())) {
			logger.debug("Received 200 response");

		} else {
			String json = null;
			HpcExceptionDTO exception = null;
			try {
				json = objectMapper.writeValueAsString(response.getBody());
				exception = objectMapper.readValue(json, HpcExceptionDTO.class);
			} catch (IOException e) {
				logger.error(
						"[PermissionArchiveTask] Failed to set archive permission, responseCode {}, can't parse message",
						response.getStatusCode());
			}
			logger.error("[PermissionArchiveTask] Failed to set archive permission, responseCode {}, message {}",
					response.getStatusCode(), exception == null ? "" : exception.getMessage());
			return false;
		}
		return true;
	}

}

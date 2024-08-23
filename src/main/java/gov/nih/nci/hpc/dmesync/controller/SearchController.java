package gov.nih.nci.hpc.dmesync.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWebException;

@Controller
public class SearchController {

	@Autowired
	private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;

	@Value("${application.name}")
	String appName;

	@Value("${dmesync.db.access:local}")
	private String access;

	@GetMapping("/search")
	public String home(Model model) {
		model.addAttribute("appName", appName);
		return "search";
	}

	@PostMapping("/search")
	public ResponseEntity<?> search(HttpSession session, @RequestHeader HttpHeaders headers, HttpServletRequest request)
			throws DmeSyncWebException {
		List<StatusInfo> results = dmeSyncWorkflowService.getService(access).findAllStatusInfoLikeOriginalFilePath("%");
		return new ResponseEntity<>(results, HttpStatus.OK);
	}
}

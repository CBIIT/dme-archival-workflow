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
import org.springframework.web.bind.annotation.RequestHeader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncStats;

@Controller
public class DashboardController {

	@Value("${application.name}")
	String appName;

	@GetMapping("/dashboard")
	public String home(Model model) {
		model.addAttribute("appName", appName);
		return "dashboard";
	}
	
	@Autowired
	private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;
	
	@GetMapping(value = "/stats")
	public ResponseEntity<?> getStats(HttpSession session, @RequestHeader HttpHeaders headers, HttpServletRequest request) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		List<DmeSyncStats> results = dmeSyncWorkflowService.getService("local").getProfileStatistics("csb", 30L);
		return new ResponseEntity<>(mapper.writeValueAsString(results), HttpStatus.OK);
	}
}

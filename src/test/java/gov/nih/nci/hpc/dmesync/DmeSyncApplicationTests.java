package gov.nih.nci.hpc.dmesync;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@Ignore
@RunWith(SpringRunner.class)
@SpringBootTest({"hpc.server.url=https://fr-s-hpcdm-gp-d.ncifcrf.gov:7738/hpc-server", "auth.token=xxxx"})
public class DmeSyncApplicationTests {

  @Test
  public void contextLoads() {}
}

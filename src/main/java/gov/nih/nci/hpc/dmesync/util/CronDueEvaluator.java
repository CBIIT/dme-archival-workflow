package gov.nih.nci.hpc.dmesync.util;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

@Component
public class CronDueEvaluator {
    public boolean isDue(DocConfig doc, Instant now, Instant lastScheduled) {
        if (doc.getCronExpression() == null || doc.getCronExpression().isEmpty()) {
            return false;
        }
        CronExpression cron = CronExpression.parse(doc.getCronExpression());
        ZonedDateTime nowWithZone = ZonedDateTime.ofInstant(now, ZoneId.systemDefault());
        ZonedDateTime epoch = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault());
        ZonedDateTime next = cron.next(lastScheduled != null ? ZonedDateTime.ofInstant(lastScheduled, ZoneId.systemDefault()) : epoch);
        return next != null && !nowWithZone.isBefore(next);
    }
}

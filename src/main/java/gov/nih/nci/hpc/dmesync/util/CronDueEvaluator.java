package gov.nih.nci.hpc.dmesync.util;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

/**
 * Evaluates whether a DOC is due to run at a given instant using its cron expression.
 */
@Component
public class CronDueEvaluator {
    /**
     * Determines whether the provided DOC should run at {@code now}.
     *
     * <p>If {@code lastScheduled} is {@code null}, the cron schedule is evaluated from Unix epoch.
     * Cron evaluation uses the JVM default system time zone.
     *
     * @param doc DOC configuration containing the cron expression
     * @param now current instant used for due evaluation
     * @param lastScheduled last scheduled execution instant, or {@code null} if never scheduled
     * @return {@code true} when the next scheduled cron instant is at or before {@code now};
     *         {@code false} otherwise or when cron is missing
     */
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
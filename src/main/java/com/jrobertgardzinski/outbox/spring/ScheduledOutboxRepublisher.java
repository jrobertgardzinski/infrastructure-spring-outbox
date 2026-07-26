package com.jrobertgardzinski.outbox.spring;

import com.jrobertgardzinski.outbox.OutboxRepublisher;
import com.jrobertgardzinski.outbox.RepublisherSettings;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * The republisher on a Spring schedule: one pass every {@code outbox.republish-interval-ms}
 * (default 15s) — re-send what was never confirmed, reap what has demonstrably been delivered.
 *
 * <p>{@code fixedDelay}, not {@code fixedRate}: the delay is measured from the END of the previous
 * pass, so a pass that runs long (a broker that answers slowly, a backlog draining) cannot have the
 * next one queued up behind it. With {@code fixedRate} a slow broker would pile passes on top of each
 * other and re-send the same rows concurrently.
 *
 * <p>Wiring, and what stays the service's decision:
 * <pre>
 *   &#64;Bean
 *   &#64;ConditionalOnProperty(name = "myservice.kafka-enabled", havingValue = "true")
 *   ScheduledOutboxRepublisher outboxRepublisher(SpringOutbox outbox,
 *           &#64;Value("${myservice.outbox.retention-hours:24}") long retentionHours) {
 *       return new ScheduledOutboxRepublisher(new OutboxRepublisher(outbox.outbox(), outbox.publisher(),
 *               RepublisherSettings.defaults(
 *                       OutboxDials.retentionHours("myservice.outbox.retention-hours", retentionHours))));
 *   }
 * </pre>
 * Two deliberate omissions. There is no {@code @Component}: the service declares the bean, so the
 * scheduler exists exactly where a broker does and not, say, in tests. And there is no
 * {@code @EnableScheduling} here either — a library has no business switching on a whole framework
 * subsystem in its host; the service enables scheduling next to the bean that needs it.
 */
public final class ScheduledOutboxRepublisher {

    /**
     * The interval a service should keep in mind when choosing
     * {@link RepublisherSettings#minAge()}: a row becomes eligible on the first pass after it ages
     * past {@code minAge}, so the effective delay before a re-send is {@code minAge} plus up to one
     * interval.
     */
    public static final String INTERVAL_PROPERTY = "outbox.republish-interval-ms";

    private final OutboxRepublisher republisher;

    public ScheduledOutboxRepublisher(OutboxRepublisher republisher) {
        this.republisher = republisher;
    }

    /**
     * One pass. Never throws — {@link OutboxRepublisher#runOnce()} swallows and logs, which matters
     * here specifically: Spring's scheduler abandons a {@code fixedDelay} task whose invocation threw,
     * so a single bad database moment would otherwise retire the durability mechanism for the lifetime
     * of the process.
     */
    @Scheduled(fixedDelayString = "${outbox.republish-interval-ms:15000}")
    public void pass() {
        republisher.runOnce();
    }
}

package com.jrobertgardzinski.outbox.spring;

import com.jrobertgardzinski.outbox.Dispatch;
import com.jrobertgardzinski.outbox.OutboxEvent;
import com.jrobertgardzinski.outbox.OutboxPublisher;
import com.jrobertgardzinski.outbox.OutboxTable;
import com.jrobertgardzinski.outbox.TransactionalOutbox;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Clock;

/**
 * The outbox as a Spring bean: <strong>{@link #announce} is the one call a Spring service makes.</strong>
 * It writes the event into the ambient {@code @Transactional} unit of work and schedules the first
 * delivery attempt for after that transaction commits.
 *
 * <p>This class exists so that the two things a service can most easily get wrong are not the
 * service's to get right:
 * <ul>
 *   <li><strong>Joining the transaction.</strong> {@link DataSourceUtils#getConnection} returns the
 *       connection Spring has bound to the current transaction, if there is one — so the outbox row
 *       is in the same unit of work as the business write, and a rollback takes it with it. Outside a
 *       transaction it hands out a fresh autocommit connection and the row simply commits on its own,
 *       which is the correct behaviour when there is nothing to be atomic with. The matching
 *       {@link DataSourceUtils#releaseConnection} is what makes both cases safe: it CLOSES a
 *       standalone connection (back to the pool) and leaves a transaction-bound one alone —
 *       try-with-resources here would close the transaction's own connection out from under it.</li>
 *   <li><strong>Not publishing too early.</strong> The send is parked on a transaction
 *       synchronisation ({@link AfterCommit}), so a rollback drops it instead of announcing an event
 *       that never happened.</li>
 * </ul>
 *
 * <p>Not annotated as a {@code @Component} on purpose. The service declares it in its own
 * configuration, where the conditions belong — the reference implementation only wants an outbox when
 * a broker exists ({@code @ConditionalOnProperty}), and a library that forced component scanning of
 * its own package would take that decision away.
 */
public final class SpringOutbox {

    private final DataSource dataSource;
    private final TransactionalOutbox outbox;
    private final OutboxPublisher publisher;

    /**
     * @param dataSource the service's {@code DataSource} — the SAME one its transaction manager
     *                   drives, or the connection this class borrows would not be the transaction's
     * @param table      the outbox table, created by the service's own Flyway migration from
     *                   {@link OutboxTable#ddl()}
     * @param clock      the service's {@code Clock} bean (a steerable one in tests)
     * @param dispatch   the send, over whatever producer the service already holds
     */
    public SpringOutbox(DataSource dataSource, OutboxTable table, Clock clock, Dispatch dispatch) {
        this.dataSource = dataSource;
        // The non-transactional legs (mark, poll, reap) get their own connections straight from the
        // DataSource, NEVER through DataSourceUtils: the mark runs on the producer's I/O thread when
        // the acknowledgement arrives, and must not enlist in whatever transaction happens to be
        // bound to that thread.
        this.outbox = new TransactionalOutbox(table, dataSource::getConnection, clock);
        this.publisher = new OutboxPublisher(outbox, dispatch);
    }

    /**
     * Writes the event in the current transaction and delivers it after the commit — the whole
     * outbox handshake in one call.
     *
     * <p>The write may throw ({@code OutboxException}), and it should: an announcement that could not
     * be made durable must take the business transaction down with it rather than let it commit
     * silently un-announced. The delivery attempt, by contrast, never throws and never blocks on the
     * broker; a failed first attempt just leaves the row for the republisher.
     */
    public void announce(OutboxEvent event) {
        append(event);
        AfterCommit.runOrDeferToCommit(() -> publisher.publishWithoutWaiting(event));
    }

    /**
     * Just the row, in the current transaction — for the rare caller that wants to decide for itself
     * when (or whether) the first attempt happens. Most callers want {@link #announce}.
     */
    public void append(OutboxEvent event) {
        Connection connection = DataSourceUtils.getConnection(dataSource);
        try {
            outbox.append(connection, event);
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource);
        }
    }

    /** The core store, for the republisher bean. */
    public TransactionalOutbox outbox() {
        return outbox;
    }

    /** The publisher, for the republisher bean. */
    public OutboxPublisher publisher() {
        return publisher;
    }
}

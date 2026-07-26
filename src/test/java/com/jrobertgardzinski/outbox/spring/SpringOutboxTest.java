package com.jrobertgardzinski.outbox.spring;

import com.jrobertgardzinski.outbox.Dispatch;
import com.jrobertgardzinski.outbox.DispatchOutcome;
import com.jrobertgardzinski.outbox.OutboxEvent;
import com.jrobertgardzinski.outbox.OutboxRepublisher;
import com.jrobertgardzinski.outbox.OutboxTable;
import com.jrobertgardzinski.outbox.RepublisherSettings;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Spring half of property (a) and of the after-commit publication — proven against a REAL Spring
 * transaction manager over a real H2, not against a mock.
 *
 * <p>That matters because the two things this adapter exists for are precisely the two that a mock
 * cannot check: whether {@code DataSourceUtils} really handed back the connection the transaction is
 * bound to (a mock would happily accept any connection and the row would commit on its own, invisibly
 * surviving a rollback), and whether the send really waited for the commit. So the fixture is a
 * {@link DataSourceTransactionManager} and a {@link TransactionTemplate} — no Spring Boot, no
 * container, no service.
 */
class SpringOutboxTest {

    private static final AtomicLong DATABASES = new AtomicLong();

    private DataSource dataSource;
    private TransactionTemplate tx;
    private SpringOutbox outbox;
    private final List<OutboxEvent> sent = new ArrayList<>();
    private boolean brokerConfirms = true;

    @BeforeEach
    void freshOutbox() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:spring-outbox-" + DATABASES.incrementAndGet()
                + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE");
        this.dataSource = h2;
        OutboxTable table = OutboxTable.named("meme_events_outbox");
        for (String statement : table.ddl().split(";")) {
            if (!statement.isBlank()) {
                execute(statement);
            }
        }
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        sent.clear();
        brokerConfirms = true;
        Dispatch broker = (event, outcome) -> {
            sent.add(event);
            if (brokerConfirms) {
                outcome.confirmed();
            } else {
                outcome.failed(new IllegalStateException("broker down"));
            }
        };
        this.outbox = new SpringOutbox(dataSource, table,
                Clock.fixed(Instant.parse("2026-07-26T10:00:00Z"), ZoneOffset.UTC), broker);
    }

    private static OutboxEvent event(String id, String key) {
        return new OutboxEvent(id, "memes-events", "MEME_DELETED", key, "cid-1",
                "{\"type\":\"MEME_DELETED\",\"memeId\":\"" + key + "\",\"eventId\":\"" + id + "\"}");
    }

    @Test
    @DisplayName("(a) a rolled-back transaction leaves NO row and publishes NO event")
    void rollback_discards_row_and_event() {
        tx.executeWithoutResult(status -> {
            outbox.announce(event("e-1", "still-alive"));
            status.setRollbackOnly();
        });

        assertEquals(0, rows(), "the row must share the fate of the transaction that wrote it —"
                + " which only holds if the append really joined it");
        assertTrue(sent.isEmpty(), "and the parked publication must be dropped with it");
    }

    @Test
    @DisplayName("(a) inside a transaction the event waits for the commit, then goes out and is marked")
    void commit_releases_the_event() {
        tx.executeWithoutResult(status -> {
            outbox.announce(event("e-2", "gone-for-good"));
            assertTrue(sent.isEmpty(), "not yet — the transaction could still roll back");
            // rows() reads through a SEPARATE connection, so this is what is actually committed
            assertEquals(0, rows(), "…and the row is not visible to anyone else either");
        });

        assertEquals(1, sent.size());
        assertEquals("memes-events", sent.getFirst().topic());
        assertTrue(published("e-2"), "a confirmed delivery must mark the row");
    }

    @Test
    @DisplayName("(a) outside a transaction the row commits on its own and the event goes out immediately")
    void without_a_transaction_the_event_goes_out_now() {
        outbox.announce(event("e-3", "already-committed"));

        assertEquals(1, sent.size());
        assertEquals(1, rows());
        assertTrue(published("e-3"));
    }

    @Test
    @DisplayName("(b) a send the broker never confirms leaves the row unpublished — the republisher's cue")
    void an_unconfirmed_send_leaves_the_row_owed() {
        brokerConfirms = false;

        tx.executeWithoutResult(status -> outbox.announce(event("e-4", "stranded")));

        assertEquals(1, sent.size());
        assertEquals(1, rows(), "the event must survive the failed send");
        assertFalse(published("e-4"));
    }

    @Test
    @DisplayName("a failed append surfaces out of the transaction, so the business change cannot commit un-announced")
    void a_failed_append_takes_the_transaction_down() {
        outbox.announce(event("duplicate", "first"));   // committed on its own

        assertThrows(RuntimeException.class, () -> tx.executeWithoutResult(status ->
                        outbox.announce(event("duplicate", "second"))),
                "the append failure must surface out of the transaction, not be swallowed");
        assertEquals(1, rows(), "the second transaction rolled back");
    }

    @Test
    @DisplayName("the republisher's pass delivers what the after-commit attempt could not")
    void the_republisher_picks_up_what_was_left() {
        brokerConfirms = false;
        tx.executeWithoutResult(status -> outbox.announce(event("e-5", "orphan")));
        assertFalse(published("e-5"));
        backdateBySeconds(60);
        brokerConfirms = true;

        // the same pass a ScheduledOutboxRepublisher bean would run every 15s
        OutboxRepublisher.Pass pass = new OutboxRepublisher(outbox.outbox(), outbox.publisher(),
                RepublisherSettings.defaults(Duration.ofHours(24))).runOnce();

        assertEquals(new OutboxRepublisher.Pass(0, 1, 1), pass);
        assertTrue(published("e-5"));
    }

    @Test
    @DisplayName("a parked action that fails does not cut down the ones parked behind it")
    void one_broken_after_commit_action_does_not_starve_the_others() {
        // Spring stops invoking synchronizations at the first exception, so without the isolation in
        // AfterCommit a failing side effect elsewhere in the service would silently swallow the
        // outbox publication parked behind it.
        List<String> ran = new ArrayList<>();
        tx.executeWithoutResult(status -> {
            AfterCommit.runOrDeferToCommit(() -> {
                throw new IllegalStateException("some other side effect blew up");
            });
            AfterCommit.runOrDeferToCommit(() -> ran.add("the outbox publication"));
        });

        assertEquals(List.of("the outbox publication"), ran);
    }

    @Test
    @DisplayName("a parked action registered from INSIDE the after-commit phase still runs")
    void a_late_registration_still_runs() {
        // Spring has already snapshotted this phase's synchronizations, so an afterCommit-only
        // listener registered here would never fire. AfterCommit listens on afterCompletion too,
        // whose snapshot Spring takes fresh.
        List<String> ran = new ArrayList<>();
        tx.executeWithoutResult(status -> AfterCommit.runOrDeferToCommit(
                () -> AfterCommit.runOrDeferToCommit(() -> ran.add("nested"))));

        assertEquals(List.of("nested"), ran);
    }

    private void execute(String sql) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException failed) {
            throw new IllegalStateException("fixture SQL failed: " + sql, failed);
        }
    }

    private int rows() {
        return queryInt("SELECT COUNT(*) FROM meme_events_outbox");
    }

    private boolean published(String id) {
        return queryInt("SELECT COUNT(*) FROM meme_events_outbox WHERE id = '" + id
                + "' AND published = TRUE") == 1;
    }

    private void backdateBySeconds(long seconds) {
        execute("UPDATE meme_events_outbox SET created_at = DATEADD('SECOND', -" + seconds
                + ", created_at)");
    }

    private int queryInt(String sql) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement query = connection.prepareStatement(sql);
             ResultSet rows = query.executeQuery()) {
            rows.next();
            return rows.getInt(1);
        } catch (SQLException failed) {
            throw new IllegalStateException("fixture query failed: " + sql, failed);
        }
    }
}

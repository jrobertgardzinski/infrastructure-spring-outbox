package com.jrobertgardzinski.outbox.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * "Run this once the transaction has committed — or right now, if there is no transaction."
 *
 * <p>The outbox needs exactly this for the first delivery attempt: the event only becomes true at
 * the commit, so publishing it any earlier would let a rollback un-announce something a consumer has
 * already acted on. A rollback must simply drop the attempt, which is what a transaction
 * synchronisation does for free.
 *
 * <p>Two traps this class dodges, both learned the hard way in the reference implementation.
 *
 * <p><strong>Late registration.</strong> A parked action may itself park another one (an after-commit
 * side effect that opens its own work). Spring has already taken this phase's synchronisation
 * snapshot by then, so an {@code afterCommit}-only listener registered from inside the after-commit
 * phase would never run. Spring does take a FRESH snapshot for {@code afterCompletion}, so the
 * listener below implements BOTH phases and runs at whichever arrives first. (Spring clears the
 * synchronisation list before invoking {@code afterCompletion}, so an attempt to register from THAT
 * phase falls through to the run-now branch instead of parking into the void.)
 *
 * <p><strong>No "already committed, so run inline" shortcut.</strong> The active-transaction check
 * runs unconditionally. During the after-commit phase the just-committed transaction still reads as
 * active, and an action running there may open a REQUIRES_NEW transaction whose own side effect must
 * park on the NEW transaction — with a shortcut in front, that side effect fired before the new
 * transaction committed, resurrecting the exact bug this class exists to prevent.
 */
public final class AfterCommit {

    private static final Logger LOG = LoggerFactory.getLogger(AfterCommit.class);

    private AfterCommit() {
    }

    public static void runOrDeferToCommit(Runnable action) {
        if (TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new ParkedAction(action));
            return;
        }
        action.run();
    }

    /**
     * Best effort and isolated. Once the commit has happened there is no caller left to tell, and a
     * failing action must not cut down the OTHER parked actions — Spring stops invoking
     * synchronisations at the first exception, so one broken side effect would silently swallow the
     * outbox publication parked behind it.
     *
     * <p>{@code Throwable}, not {@code RuntimeException}: an {@code Error} would otherwise abort the
     * remaining synchronisations too. Swallowing an {@code Error} is normally taboo; here isolation
     * wins, the log gets the truth at ERROR level, and a JVM sick enough for it to matter will fail
     * the next request on its own.
     */
    private static final class ParkedAction implements TransactionSynchronization {

        private final Runnable action;
        private boolean done;

        private ParkedAction(Runnable action) {
            this.action = action;
        }

        @Override
        public void afterCommit() {
            runOnce();
        }

        @Override
        public void afterCompletion(int status) {
            if (status == STATUS_COMMITTED) {
                runOnce();
            }
            // a rollback reaches only this method, with a non-committed status: the parked action is
            // silently dropped, which is the whole point of parking it
        }

        private void runOnce() {
            if (done) {
                return;
            }
            done = true;
            try {
                action.run();
            } catch (Error catastrophe) {
                LOG.error("after-commit action failed with an Error — swallowed only so the remaining"
                        + " parked actions still run; the JVM may be unhealthy", catastrophe);
            } catch (Throwable failed) {
                LOG.warn("after-commit action failed — the transaction is committed, continuing with"
                        + " the remaining ones", failed);
            }
        }
    }
}

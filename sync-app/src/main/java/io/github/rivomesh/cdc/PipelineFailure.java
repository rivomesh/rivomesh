package io.github.rivomesh.cdc;

/** Identifies the failing part of a pipeline without exposing driver messages or row data. */
public class PipelineFailure extends Exception {
    public enum Phase {
        SOURCE_CONNECT, SOURCE_DISCOVERY, SOURCE_READ, SOURCE_DECODE, SOURCE_SNAPSHOT,
        DDL, NORMALIZE, JOURNAL_STORAGE, JOURNAL_ACK
    }

    public final Phase phase;

    public PipelineFailure(Phase phase, Throwable cause) {
        super(phase.name(), cause);
        this.phase = phase;
    }

    public static Phase phase(Throwable failure, Phase fallback) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            if (cause instanceof PipelineFailure) return ((PipelineFailure) cause).phase;
        return fallback;
    }
}

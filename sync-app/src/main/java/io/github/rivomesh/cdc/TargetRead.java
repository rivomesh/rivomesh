package io.github.rivomesh.cdc;

import java.util.List;

/** A complete, bounded table scan protected by the target's physical checkpoint fence. */
public interface TargetRead extends AutoCloseable {
    final class Row {
        public String rowId;
        public List<Cell> values;
    }
    /** Empty means EOF; a caller must read EOF before declaring a table complete. */
    List<Row> next(int maximumRows)throws Exception;
    void verify()throws Exception;
    @Override void close()throws Exception;
}

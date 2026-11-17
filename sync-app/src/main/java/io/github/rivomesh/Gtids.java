package io.github.rivomesh;

import com.github.shyiko.mysql.binlog.GtidSet;

final class Gtids {
    private Gtids() {}
    static GtidSet parse(String value) {
        String normalized = value == null ? "" : value.replaceAll("\\s", "");
        for (String part : normalized.split(",")) {
            if (!part.isEmpty() && !part.matches("[0-9a-fA-F-]{36}(?::[0-9]+(?:-[0-9]+)?)+"))
                throw new IllegalArgumentException("GTID 格式不支持；当前仅支持非 tagged MySQL GTID");
        }
        return new GtidSet(normalized);
    }
    static boolean contains(String superset, String subset) { return parse(subset).isContainedWithin(parse(superset)); }
    static String union(String a, String b) {
        GtidSet left = parse(a), right = parse(b);
        if (left.isContainedWithin(right)) return right.toString();
        if (right.isContainedWithin(left)) return left.toString();
        java.util.Map<String, java.util.List<GtidSet.Interval>> all = new java.util.TreeMap<>();
        for (GtidSet set : new GtidSet[]{left, right}) for (GtidSet.UUIDSet uuid : set.getUUIDSets())
            all.computeIfAbsent(uuid.getUUID(), k -> new java.util.ArrayList<>()).addAll(uuid.getIntervals());
        java.util.List<String> parts = new java.util.ArrayList<>();
        for (java.util.Map.Entry<String, java.util.List<GtidSet.Interval>> entry : all.entrySet()) {
            entry.getValue().sort(java.util.Comparator.comparingLong(GtidSet.Interval::getStart));
            StringBuilder part = new StringBuilder(entry.getKey());
            long start = -1, end = -1;
            for (GtidSet.Interval interval : entry.getValue()) {
                if (start < 0) { start = interval.getStart(); end = interval.getEnd(); }
                else if (interval.getStart() <= end || interval.getStart() == end + 1) end = Math.max(end, interval.getEnd());
                else { part.append(':').append(start).append('-').append(end); start = interval.getStart(); end = interval.getEnd(); }
            }
            if (start >= 0) part.append(':').append(start).append('-').append(end);
            parts.add(part.toString());
        }
        return parse(String.join(",", parts)).toString();
    }
}

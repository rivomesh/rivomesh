package io.github.rivomesh;

import com.github.shyiko.mysql.binlog.GtidSet;
import java.util.regex.Pattern;

final class Gtids {
    private Gtids() {}
    private static final Pattern CLASSICAL=Pattern.compile("[0-9a-fA-F-]{36}(?::[0-9]+(?:-[0-9]+)?)+");
    private static final Pattern TAG=Pattern.compile(":[A-Za-z_][A-Za-z0-9_]{0,31}:[0-9]");
    static GtidSet parse(String value) {
        String normalized = value == null ? "" : value.replaceAll("\\s", "");
        for (String part : normalized.split(",")) {
            if(!part.isEmpty()&&!CLASSICAL.matcher(part).matches()){
                if(TAG.matcher(part).find())throw new IllegalArgumentException("MYSQL_TAGGED_GTID_UNSUPPORTED");
                throw new IllegalArgumentException("MYSQL_GTID_FORMAT_INVALID");
            }
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

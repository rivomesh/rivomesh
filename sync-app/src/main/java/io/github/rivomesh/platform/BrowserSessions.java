package io.github.rivomesh.platform;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.*;

/** Opaque browser credentials. No API token or raw session ID is retained in the store. */
final class BrowserSessions implements AutoCloseable {
    interface Store extends AutoCloseable {
        Session insert(String digest, Session session);
        Session find(String digest, long now, long idleMillis);
        void remove(String digest);
        void touch(String digest, long now);
        void close();
    }
    static final class MemoryStore implements Store {
        private final Map<String,Session> sessions = new HashMap<>();
        private final int maximum;
        MemoryStore(int maximum) { this.maximum = maximum; }
        public synchronized Session insert(String digest, Session session) {
            sessions.values().removeIf(s -> session.lastSeen >= s.expiresAt || session.lastSeen - s.lastSeen >= session.idleMillis);
            if (sessions.size() >= maximum) throw new TaskService.Conflict("SESSION_CAPACITY_REACHED");
            sessions.put(digest, session); return session;
        }
        public synchronized Session find(String digest, long now, long idleMillis) {
            Session session = sessions.get(digest);
            if (session != null && (now >= session.expiresAt || now - session.lastSeen >= idleMillis)) { sessions.remove(digest); return null; }
            return session;
        }
        public synchronized void touch(String digest, long now) { Session s = sessions.get(digest); if (s != null) s.lastSeen = Math.max(s.lastSeen, now); }
        public synchronized void remove(String digest) { sessions.remove(digest); }
        public synchronized void close() { sessions.clear(); }
    }
    static final class Session {
        final AccessControl.Principal principal;
        final String csrf;
        final long expiresAt, idleMillis, credentialExpiresAt;
        long lastSeen;
        Session(AccessControl.Principal principal, String csrf, long now, long absoluteMillis, long idleMillis) {
            this(principal,csrf,now,absoluteMillis,idleMillis,Long.MAX_VALUE);
        }
        Session(AccessControl.Principal principal, String csrf, long now, long absoluteMillis, long idleMillis, long credentialExpiresAt) {
            this.principal = principal; this.csrf = csrf; lastSeen = now;
            this.credentialExpiresAt=credentialExpiresAt; expiresAt=Math.min(now+absoluteMillis,credentialExpiresAt);
            this.idleMillis=idleMillis;
        }
        Map<String,Object> view() {
            Map<String,Object> view = principal.view(); view.put("authentication", "COOKIE"); view.put("csrfToken", csrf);
            view.put("expiresAt", java.time.Instant.ofEpochMilli(expiresAt).toString()); return view;
        }
    }
    static final class Issued {
        final String id; final Session session;
        Issued(String id, Session session) { this.id = id; this.session = session; }
    }
    private final PlatformConfig.Sessions config;
    private final Store store;
    private final Clock clock;
    private final SecureRandom random = new SecureRandom();
    BrowserSessions(PlatformConfig.Sessions config) { this(config, new MemoryStore(config.maximum), Clock.systemUTC()); }
    BrowserSessions(PlatformConfig.Sessions config, Store store, Clock clock) { this.config = config; this.store = store; this.clock = clock; }
    Issued create(AccessControl.Principal principal) {
        return create(principal,Long.MAX_VALUE);
    }
    Issued create(AccessControl.Principal principal,long credentialExpiresAt) {
        long now=clock.millis();
        if(credentialExpiresAt<=now)throw new TaskService.Conflict("AUTHORIZATION_CHANGED");
        String id=secret();
        Session session=new Session(principal,secret(),now,config.absoluteSeconds*1000L,config.idleSeconds*1000L,credentialExpiresAt);
        session=store.insert(AccountDirectory.hash(id), session); return new Issued(id, session);
    }
    Session find(String id) { return valid(id) ? store.find(AccountDirectory.hash(id), clock.millis(), config.idleSeconds * 1000L) : null; }
    void touch(String id) { if (valid(id)) store.touch(AccountDirectory.hash(id), clock.millis()); }
    void revoke(String id) { if (valid(id)) store.remove(AccountDirectory.hash(id)); }
    private String secret() { byte[] bytes = new byte[32]; random.nextBytes(bytes); return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes); }
    private static boolean valid(String value) { return value != null && value.matches("[A-Za-z0-9_-]{43}"); }
    static boolean csrf(Session session, String value) {
        return session != null && valid(value) && MessageDigest.isEqual(session.csrf.getBytes(StandardCharsets.US_ASCII), value.getBytes(StandardCharsets.US_ASCII));
    }
    String cookieName() { return config.secureCookie ? "__Host-rivomesh-session" : "rivomesh-dev-session"; }
    String cookie(String id) {return cookie(id,config.absoluteSeconds);}
    String credentialCookie(Issued issued){
        return cookie(issued.id,Math.max(0,(issued.session.expiresAt-clock.millis())/1000));
    }
    private String cookie(String id,long seconds) {
        return cookieName() + "=" + (id == null ? "" : id) + "; Path=/; HttpOnly; SameSite=Strict" + (config.secureCookie ? "; Secure" : "")
                + (id == null ? "; Max-Age=0" : config.rememberBrowser ? "; Max-Age=" + seconds : "");
    }
    String cookieId(List<String> headers) {return readCookie(headers,cookieName());}
    static String readCookie(List<String> headers,String name) {
        String found = null;
        if (headers != null) for (String header : headers) for (String part : header.split(";")) {
            String[] pair = part.trim().split("=", 2);
            if (pair.length == 2 && pair[0].equals(name)) { if (found != null) throw new IllegalArgumentException("SESSION_COOKIE_AMBIGUOUS"); found = pair[1]; }
        }
        return found;
    }
    public void close() { store.close(); }
}

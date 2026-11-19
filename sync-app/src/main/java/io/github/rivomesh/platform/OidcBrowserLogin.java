package io.github.rivomesh.platform;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rivomesh.Model;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Authorization code login with one configured issuer and one pinned signing key. */
final class OidcBrowserLogin {
    static final String ROOT="/auth/oidc/", START=ROOT+"start", CALLBACK=ROOT+"callback";
    static final String COOKIE="__Host-rivomesh-oidc";
    private static final int ATTEMPT_SECONDS=300, MAX_DOCUMENT=48*1024, MAX_ID_TOKEN=24*1024;
    private static final Duration REQUEST_LIMIT=Duration.ofSeconds(8);
    private static final ObjectMapper STRICT=new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Base64.Encoder URL64=Base64.getUrlEncoder().withoutPadding();

    static final class Denied extends RuntimeException {}
    static final class Unavailable extends RuntimeException {}
    private record Attempt(String state,String nonce,String verifier,String previousSession) {}
    record Start(String location,String binding) {}
    record Login(AccountDirectory.ExternalIdentity identity,long expiresAt,String previousSession) {}

    private final PlatformConfig.Oidc settings;
    private final SharedAccessStore store;
    private final ControlCipher cipher;
    private final String redirect,scope;
    private final RSAPublicKey signingKey;
    private final HttpClient http;
    private final Clock clock;
    private final SecureRandom random=new SecureRandom();

    OidcBrowserLogin(PlatformConfig config,SharedAccessStore store,HttpClient transport,Clock clock){
        validate(config);
        this.settings=config.oidc;
        this.store=store;
        this.clock=clock;
        this.cipher=new ControlCipher(config.cluster.taskKey,config.cluster.id,store.identity);
        this.redirect=config.sessions.publicOrigin+CALLBACK;
        this.signingKey=key(settings.rsaPublicKey);
        this.http=transport==null?HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(4))
                .followRedirects(HttpClient.Redirect.NEVER).build():transport;
        this.scope="oidc-browser/"+AccountDirectory.hash(String.join("\n", settings.issuer,settings.authorizationEndpoint,
                settings.tokenEndpoint,settings.clientId,settings.keyId,settings.rsaPublicKey,redirect));
    }

    static void validate(PlatformConfig config){
        PlatformConfig.Oidc p=config.oidc;
        if(config.cluster==null||!config.cluster.sharedAccess||!config.cluster.sharedTasks
                ||config.cluster.role==ClusterConfig.Role.AGENT||!config.sessions.enabled||!config.sessions.secureCookie
                ||!validIssuer(config.sessions.publicOrigin)||!validIssuer(p.issuer)||!identifier(p.clientId,128)
                ||!identifier(p.keyId,128)||!visible(p.clientSecret,2048)||p.clientSecret.contains("${"))
            throw configuration();
        URI issuer=URI.create(p.issuer);
        for(String endpoint:new String[]{p.authorizationEndpoint,p.tokenEndpoint}){
            if(!validIssuer(endpoint)||!issuer.getRawAuthority().equals(URI.create(endpoint).getRawAuthority()))
                throw configuration();
        }
        key(p.rsaPublicKey);
    }

    private static RSAPublicKey key(String encoded){
        try{
            if(encoded==null||encoded.length()>1024)throw configuration();
            byte[] der=Base64.getDecoder().decode(encoded);
            if(!Base64.getEncoder().encodeToString(der).equals(encoded))throw configuration();
            RSAPublicKey key=(RSAPublicKey)KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(der));
            int bits=key.getModulus().bitLength();
            if(bits<2048||bits>4096)throw configuration();
            return key;
        }catch(Exception invalid){throw configuration();}
    }

    static boolean validIssuer(String value){
        if(!visible(value,1024))return false;
        try{
            URI uri=URI.create(value);
            return "https".equals(uri.getScheme())&&uri.getHost()!=null&&uri.getRawUserInfo()==null
                    &&uri.getRawQuery()==null&&uri.getRawFragment()==null&&(uri.getPort()==-1||uri.getPort()>0&&uri.getPort()<=65535);
        }catch(IllegalArgumentException invalid){return false;}
    }
    static boolean validSubject(String value){return visible(value,255);}
    private static boolean visible(String value,int maximum){
        return value!=null&&!value.isEmpty()&&value.length()<=maximum&&value.chars().allMatch(c->c>=33&&c<=126);
    }
    private static boolean identifier(String value,int maximum){
        return visible(value,maximum)&&value.matches("[A-Za-z0-9._~-]+");
    }
    private static IllegalArgumentException configuration(){return new IllegalArgumentException("OIDC_CONFIG_INVALID");}

    Start begin(String previousSession)throws Exception{
        Attempt attempt=new Attempt(secret(),secret(),secret(),opaque(previousSession)?previousSession:null);
        String binding=cipher.seal(scope,Model.JSON.writeValueAsString(attempt));
        String challenge=URL64.encodeToString(MessageDigest.getInstance("SHA-256").digest(attempt.verifier().getBytes(StandardCharsets.US_ASCII)));
        String query=form("response_type","code","scope","openid","client_id",settings.clientId,"redirect_uri",redirect,
                "state",attempt.state(),"nonce",attempt.nonce(),"code_challenge",challenge,"code_challenge_method","S256");
        store.beginOidc(AccountDirectory.hash(attempt.state()),ATTEMPT_SECONDS*1000L);
        return new Start(settings.authorizationEndpoint+"?"+query,binding);
    }

    Login finish(String rawQuery,String binding){
        Map<String,String> values=query(rawQuery);
        String state=values.get("state"),issuer=values.get("iss");
        Attempt attempt=attempt(binding);
        if(!opaque(state)||!equal(attempt.state(),state)||(issuer!=null&&!settings.issuer.equals(issuer))
                ||!store.consumeOidc(AccountDirectory.hash(state)))throw new Denied();
        String code=values.get("code");
        if(values.containsKey("error")||!visible(code,2048))throw new Denied();
        return identity(exchange(code,attempt.verifier()),attempt);
    }
    private Attempt attempt(String binding){
        if(binding==null||binding.length()>1024)throw new Denied();
        try{
            Attempt attempt=STRICT.readValue(cipher.open(scope,binding),Attempt.class);
            if(attempt==null||!opaque(attempt.state())||!opaque(attempt.nonce())||!opaque(attempt.verifier()))throw new Denied();
            return attempt;
        }catch(IOException|TaskService.Conflict malformed){throw new Denied();}
    }

    private static Map<String,String> query(String raw){
        if(raw==null||raw.isEmpty()||raw.length()>8192)throw new Denied();
        Map<String,String> values=new HashMap<>();
        try{
            for(String field:raw.split("&",-1)){
                String[] pair=field.split("=",2);
                if(pair.length!=2||!identifier(pair[0],64)
                        ||values.putIfAbsent(pair[0],URLDecoder.decode(pair[1],StandardCharsets.UTF_8))!=null||values.size()>12)
                    throw new Denied();
            }
        }catch(IllegalArgumentException malformed){throw new Denied();}
        return values;
    }

    private String exchange(String code,String verifier){
        String credentials=encoded(settings.clientId)+":"+encoded(settings.clientSecret);
        String body=form("grant_type","authorization_code","code",code,"redirect_uri",redirect,"code_verifier",verifier);
        HttpRequest request=HttpRequest.newBuilder(URI.create(settings.tokenEndpoint)).timeout(REQUEST_LIMIT)
                .header("Content-Type","application/x-www-form-urlencoded").header("Accept","application/json")
                .header("Authorization","Basic "+Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.US_ASCII)))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        var response=http.sendAsync(request,ignored->new BoundedBody());
        try{
            HttpResponse<byte[]> result=response.get(REQUEST_LIMIT.toSeconds(),TimeUnit.SECONDS);
            if(result.statusCode()==400||result.statusCode()==401)throw new Denied();
            if(result.statusCode()!=200)throw new Unavailable();
            var contentType=result.headers().firstValue("Content-Type").orElse("");
            if(!"application/json".equals(contentType.split(";",2)[0].trim().toLowerCase(Locale.ROOT)))throw new Denied();
            JsonNode json=STRICT.readTree(result.body());
            String token=json==null?null:json.path("id_token").textValue();
            if(token==null||token.length()>MAX_ID_TOKEN)throw new Denied();
            return token;
        }catch(InterruptedException interrupted){
            response.cancel(true);Thread.currentThread().interrupt();throw new Unavailable();
        }catch(TimeoutException timeout){
            response.cancel(true);throw new Unavailable();
        }catch(ExecutionException transport){throw new Unavailable();}
        catch(IOException malformed){throw new Denied();}
    }
    private static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final HttpResponse.BodySubscriber<byte[]> body=HttpResponse.BodySubscribers.ofByteArray();
        private Flow.Subscription subscription;
        private int remaining=MAX_DOCUMENT;
        public CompletionStage<byte[]> getBody(){return body.getBody();}
        public void onSubscribe(Flow.Subscription source){subscription=source;body.onSubscribe(source);}
        public void onNext(List<ByteBuffer> buffers){
            for(ByteBuffer buffer:buffers){
                if(buffer.remaining()>remaining){
                    subscription.cancel();
                    body.onError(new IOException("OIDC_BODY_LIMIT"));
                    return;
                }
                remaining-=buffer.remaining();
            }
            body.onNext(buffers);
        }
        public void onError(Throwable failure){body.onError(failure);}
        public void onComplete(){body.onComplete();}
    }

    private Login identity(String jwt,Attempt attempt){
        try{
            String[] parts=jwt.split("\\.",-1);
            if(parts.length!=3)throw new Denied();
            JsonNode header=STRICT.readTree(segment(parts[0]));
            if(header==null||!header.isObject()||!"RS256".equals(header.path("alg").textValue())
                    ||!settings.keyId.equals(header.path("kid").textValue())||header.has("crit")||header.has("b64"))
                throw new Denied();
            Signature signature=Signature.getInstance("SHA256withRSA");
            signature.initVerify(signingKey);
            signature.update((parts[0]+"."+parts[1]).getBytes(StandardCharsets.US_ASCII));
            if(!signature.verify(segment(parts[2])))throw new Denied();
            JsonNode claims=STRICT.readTree(segment(parts[1]));
            if(claims==null||!claims.isObject()||!settings.issuer.equals(claims.path("iss").textValue())
                    ||!audience(claims.path("aud"))||!equal(attempt.nonce(),claims.path("nonce").textValue())
                    ||claims.has("azp")&&!settings.clientId.equals(claims.path("azp").textValue()))
                throw new Denied();
            String subject=claims.path("sub").textValue();
            if(!validSubject(subject))throw new Denied();
            long now=clock.instant().getEpochSecond(),issued=seconds(claims.path("iat")),expires=seconds(claims.path("exp"));
            if(issued<0||issued>now+60||expires<=now||expires<=issued
                    ||claims.has("nbf")&&seconds(claims.path("nbf"))>now)
                throw new Denied();
            return new Login(AccountDirectory.ExternalIdentity.of(settings.issuer,subject),Math.multiplyExact(expires,1000),attempt.previousSession());
        }catch(Denied rejected){throw rejected;}
        catch(Exception malformed){throw new Denied();}
    }
    private boolean audience(JsonNode value){
        return settings.clientId.equals(value.textValue())
                ||value.isArray()&&value.size()==1&&settings.clientId.equals(value.get(0).textValue());
    }
    private static long seconds(JsonNode number){
        if(!number.isIntegralNumber()||!number.canConvertToLong())throw new Denied();
        return number.longValue();
    }
    private static byte[] segment(String value){
        if(value.isEmpty()||!value.matches("[A-Za-z0-9_-]+"))throw new Denied();
        byte[] decoded=Base64.getUrlDecoder().decode(value);
        if(!URL64.encodeToString(decoded).equals(value))throw new Denied();
        return decoded;
    }
    private static String form(String... entries){
        StringBuilder result=new StringBuilder();
        for(int i=0;i<entries.length;i+=2){
            if(i>0)result.append('&');
            result.append(entries[i]).append('=').append(encoded(entries[i+1]));
        }
        return result.toString();
    }
    private static String encoded(String value){return URLEncoder.encode(value,StandardCharsets.UTF_8);}
    private String secret(){byte[] bytes=new byte[32];random.nextBytes(bytes);return URL64.encodeToString(bytes);}
    private static boolean opaque(String value){return value!=null&&value.matches("[A-Za-z0-9_-]{43}");}
    private static boolean equal(String expected,String supplied){
        return supplied!=null&&MessageDigest.isEqual(expected.getBytes(StandardCharsets.US_ASCII),supplied.getBytes(StandardCharsets.UTF_8));
    }
    static String setBinding(String binding){
        return COOKIE+"="+(binding==null?"":binding)+"; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age="+(binding==null?0:ATTEMPT_SECONDS);
    }
}

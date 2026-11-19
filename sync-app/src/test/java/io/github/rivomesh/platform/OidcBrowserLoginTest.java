package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import io.github.rivomesh.Model;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class OidcBrowserLoginTest {
    private static final String ORIGIN="https://console.example.test";
    private static final String CLIENT="rivomesh-fixture", SUBJECT="provider-user:viewer";
    private static final String SECRET="local-oidc-fixture:+_secret";
    private static final String SIGNING_KEY_ID="fixture-rsa";
    private static final String ACCOUNT="/api/accounts/local-viewer/external-identity";
    private static final String VIEWER_TOKEN="oidc-viewer-local-token-fixture-only";
    private static final Base64.Encoder URL64=Base64.getUrlEncoder().withoutPadding();

    @TempDir Path directory;

    private enum Claim {VALID, BAD_NONCE, BAD_ISSUER, BAD_SIGNATURE, WRONG_SUBJECT, WRONG_AUDIENCE, EXPIRED}
    private record BrowserAttempt(String cookie,String query,Instant expiresAt,String code){}
    private record Grant(String nonce,String challenge,Claim claim,Instant expiresAt){}

    @Test void crossControllerLoginRejectsReplayAndForeignClaimsAndSharesLocalRevocation()throws Exception{
        try(Provider provider=new Provider(directory)){
            GlobalControlHttpTest cluster=new GlobalControlHttpTest();
            cluster.directory=directory;
            PlatformConfig a=configure(cluster.config("a"),provider),b=configure(cluster.config("b"),provider);
            try(TaskService first=new TaskService(a,new PlatformTest.Backend(),new PlatformTest.Workers());
                TaskService second=new TaskService(b,new PlatformTest.Backend(),new PlatformTest.Workers());
                ConsoleServer one=new ConsoleServer(a,first,null,provider.client);
                ConsoleServer two=new ConsoleServer(b,second,null,provider.client)){
                one.start();two.start();
                String left=cluster.base(one),right=cluster.base(two);
                assertTrue(json(send(provider.client,left+"/auth/oidc/available","GET",Map.of(),null),200).path("enabled").asBoolean());
                JsonNode directoryPage=json(send(provider.client,left+"/api/accounts","GET",admin(null),null),200);
                assertEquals(provider.issuer,directoryPage.path("externalIdentityIssuer").textValue());
                assertFalse(directoryPage.toString().contains(SECRET));
                assertFalse(directoryPage.toString().contains(provider.publicKey));
                cluster.create(left,"allowed","b");
                PlatformTest.await(()->cluster.completed(left,"allowed","global_create_allowed"));
                io.github.rivomesh.Config outside=PlatformTest.task("outside");
                outside.source.serverId=910002;
                ObjectNode outsideRequest=Model.JSON.createObjectNode().put("ownerNodeId","a");
                outsideRequest.set("config",Model.JSON.valueToTree(outside));
                json(send(provider.client,right+"/api/tasks","POST",admin("global_create_outside"),outsideRequest),202);
                PlatformTest.await(()->cluster.completed(right,"outside","global_create_outside"));

                ObjectNode create=Model.JSON.createObjectNode().put("expectedRevision",revision(provider.client,left))
                        .put("id","local-viewer").put("role","VIEWER").put("enabled",true).put("token",VIEWER_TOKEN);
                create.putArray("taskIds").add("allowed");
                create.putArray("nodeIds").add("b");
                json(send(provider.client,left+"/api/accounts","POST",admin("oidc_fixture_create_viewer"),create),201);

                ObjectNode bind=Model.JSON.createObjectNode().put("expectedRevision",revision(provider.client,left))
                        .put("issuer",provider.issuer).put("subject",SUBJECT);
                ObjectNode untrusted=bind.deepCopy().put("issuer","https://external.example/issuer");
                assertCode(send(provider.client,right+ACCOUNT,"PUT",admin("oidc_fixture_untrusted_issuer"),untrusted),
                        400,"ACCOUNT_EXTERNAL_ISSUER_UNTRUSTED");

                HttpResponse<String> adminLogin=send(provider.client,left+"/api/session","POST",admin(null),null);
                String adminCookie=cookie(adminLogin,"__Host-rivomesh-session");
                String csrf=json(adminLogin,200).path("csrfToken").textValue();
                Map<String,String> browserAdmin=Map.of("Cookie",adminCookie,"Origin",ORIGIN,"X-Request-ID","oidc_fixture_bind");
                assertCode(send(provider.client,right+ACCOUNT,"PUT",browserAdmin,bind),403,"CSRF_REJECTED");
                Map<String,String> checkedAdmin=new HashMap<>(browserAdmin);
                checkedAdmin.put("X-CSRF-Token",csrf);
                JsonNode bound=json(send(provider.client,right+ACCOUNT,"PUT",checkedAdmin,bind),200);
                assertFalse(bound.toString().contains(SUBJECT));
                assertEquals(provider.issuer,bound.path("externalIdentityIssuer").textValue());
                JsonNode mapping=bound.path("users").get(1).path("externalIdentity");
                assertEquals(provider.issuer,mapping.path("issuer").textValue());
                assertEquals(AccountDirectory.hash(SUBJECT),mapping.path("subjectSha256").textValue());
                assertCode(send(provider.client,left+"/api/session","GET",Map.of("Cookie",adminCookie),null),401,"UNAUTHORIZED");

                HttpResponse<String> oldViewer=send(provider.client,right+"/api/session","POST",Map.of("Authorization","Bearer "+VIEWER_TOKEN),null);
                assertEquals(200,oldViewer.statusCode(),oldViewer.body());
                String oldSession=cookie(oldViewer,"__Host-rivomesh-session");
                BrowserAttempt attempt=provider.begin(left,Claim.VALID,Map.of("Cookie",oldSession));
                String callback=right+OidcBrowserLogin.CALLBACK+"?"+attempt.query;
                assertCode(send(provider.client,callback,"GET",Map.of(),null),401,"OIDC_LOGIN_DENIED");
                assertCode(send(provider.client,right+OidcBrowserLogin.CALLBACK+"?"+attempt.query+"&state=repeat","GET",
                        Map.of("Cookie",attempt.cookie),null),401,"OIDC_LOGIN_DENIED");
                assertEquals(0,provider.exchanges.get());

                HttpResponse<String> signedIn=provider.finish(attempt,right);
                assertEquals(303,signedIn.statusCode(),signedIn.body());
                assertEquals("/",signedIn.headers().firstValue("Location").orElseThrow());
                assertEquals(1,provider.exchanges.get());
                assertTrue(signedIn.headers().allValues("Set-Cookie").stream().anyMatch(h->h.startsWith(OidcBrowserLogin.COOKIE+"=;")&&h.contains("Max-Age=0")));
                assertTrue(signedIn.headers().allValues("Set-Cookie").stream().anyMatch(h->h.startsWith("__Host-rivomesh-session=")&&h.contains("SameSite=Strict")&&h.contains("Secure")&&h.contains("HttpOnly")));
                String session=cookie(signedIn,"__Host-rivomesh-session");
                assertNotEquals(oldSession,session);
                Map<String,String> signed=Map.of("Cookie",session);
                assertCode(send(provider.client,left+"/api/session","GET",Map.of("Cookie",oldSession),null),401,"UNAUTHORIZED");
                for(String entry:new String[]{left,right}){
                    JsonNode identity=json(send(provider.client,entry+"/api/session","GET",signed,null),200);
                    assertEquals("local-viewer",identity.path("id").textValue());
                    assertEquals("VIEWER",identity.path("role").textValue());
                    assertEquals("allowed",identity.path("taskScope").get(0).textValue());
                    assertEquals("b",identity.path("nodeScope").get(0).textValue());
                    assertEquals(attempt.expiresAt,Instant.parse(identity.path("expiresAt").textValue()));
                    assertEquals(1,json(send(provider.client,entry+"/api/tasks","GET",signed,null),200).path("total").asInt());
                    assertCode(send(provider.client,entry+"/api/tasks/outside","GET",signed,null),404,"TASK_NOT_FOUND");
                    assertCode(send(provider.client,entry+"/api/accounts","GET",signed,null),403,"FORBIDDEN");
                }
                assertCode(provider.finish(attempt,left),401,"OIDC_LOGIN_DENIED");
                assertEquals(1,provider.exchanges.get());

                for(Claim malformed:new Claim[]{Claim.BAD_NONCE,Claim.BAD_ISSUER,Claim.BAD_SIGNATURE,Claim.WRONG_SUBJECT,Claim.WRONG_AUDIENCE,Claim.EXPIRED}){
                    BrowserAttempt wrong=provider.begin(left,malformed,Map.of());
                    assertCode(provider.finish(wrong,right),401,"OIDC_LOGIN_DENIED");
                }
                assertEquals(7,provider.exchanges.get());

                ObjectNode revoke=Model.JSON.createObjectNode().put("expectedRevision",revision(provider.client,right));
                JsonNode revoked=json(send(provider.client,right+ACCOUNT,"DELETE",admin("oidc_fixture_revoke"),revoke),200);
                assertFalse(revoked.path("users").get(1).has("externalIdentity"));
                assertCode(send(provider.client,left+"/api/session","GET",signed,null),401,"UNAUTHORIZED");
                assertCode(send(provider.client,right+"/api/session","GET",signed,null),401,"UNAUTHORIZED");
                assertCode(provider.finish(provider.begin(left,Claim.VALID,Map.of()),right),401,"OIDC_LOGIN_DENIED");

                JsonNode audit=json(send(provider.client,right+"/api/audit/export?limit=1000","GET",admin(null),null),200);
                String log=audit.toString();
                assertTrue(log.contains("ACCOUNT_EXTERNAL_BIND"));
                assertTrue(log.contains("ACCOUNT_EXTERNAL_REVOKE"));
                assertTrue(log.contains("GET_OIDC_CALLBACK"));
                assertTrue(log.contains("OIDC_LOGIN_DENIED"));
                assertFalse(log.contains(attempt.code));
                assertFalse(log.contains(attempt.cookie.substring(attempt.cookie.indexOf('=')+1)));
                assertFalse(log.contains(SUBJECT));
                assertFalse(log.contains(SECRET));
                assertEquals("VIEWER",json(send(provider.client,left+"/api/session","GET",Map.of("Authorization","Bearer "+VIEWER_TOKEN),null),200).path("role").textValue());
            }
        }
    }

    @Test void fixedHttpsProfileRejectsUnsupportedEndpointsAndModes()throws Exception{
        try(Provider provider=new Provider(directory)){
            GlobalControlHttpTest cluster=new GlobalControlHttpTest();
            cluster.directory=directory;
            PlatformConfig config=configure(cluster.config("a"),provider);
            config.validate();
            config.oidc.tokenEndpoint=provider.issuer.replace("https:","http:")+"/token";
            assertEquals("OIDC_CONFIG_INVALID",assertThrows(IllegalArgumentException.class,config::validate).getMessage());
            config.oidc.tokenEndpoint="https://different.example/token";
            assertEquals("OIDC_CONFIG_INVALID",assertThrows(IllegalArgumentException.class,config::validate).getMessage());
            config.oidc.tokenEndpoint=provider.issuer+"/token";
            config.sessions.secureCookie=false;
            config.sessions.publicOrigin="http://console.example.test";
            assertEquals("OIDC_CONFIG_INVALID",assertThrows(IllegalArgumentException.class,config::validate).getMessage());
            config.sessions.secureCookie=true;
            config.sessions.publicOrigin=ORIGIN;
            config.cluster.sharedTasks=false;
            config.cluster.taskKey=null;
            assertEquals("OIDC_CONFIG_INVALID",assertThrows(IllegalArgumentException.class,config::validate).getMessage());
        }
    }

    private static PlatformConfig configure(PlatformConfig config,Provider provider){
        config.sessions.publicOrigin=ORIGIN;
        config.sessions.secureCookie=true;
        config.sessions.rememberBrowser=true;
        config.oidc=new PlatformConfig.Oidc();
        config.oidc.issuer=provider.issuer;
        config.oidc.authorizationEndpoint=provider.issuer+"/authorize";
        config.oidc.tokenEndpoint=provider.issuer+"/token";
        config.oidc.clientId=CLIENT;
        config.oidc.clientSecret=SECRET;
        config.oidc.keyId=SIGNING_KEY_ID;
        config.oidc.rsaPublicKey=provider.publicKey;
        return config;
    }
    private static String revision(HttpClient client,String base)throws Exception{
        return json(send(client,base+"/api/accounts","GET",admin(null),null),200).path("revision").textValue();
    }
    private static Map<String,String> admin(String request){
        return request==null?Map.of("Authorization","Bearer "+PlatformTest.TOKEN):
                Map.of("Authorization","Bearer "+PlatformTest.TOKEN,"X-Request-ID",request);
    }
    private static HttpRequest request(String address,String method,Map<String,String> headers,JsonNode data){
        HttpRequest.Builder builder=HttpRequest.newBuilder(URI.create(address)).timeout(Duration.ofSeconds(12));
        headers.forEach(builder::header);
        if(data!=null)builder.header("Content-Type","application/json");
        return builder.method(method,data==null?HttpRequest.BodyPublishers.noBody():HttpRequest.BodyPublishers.ofString(data.toString())).build();
    }
    private static HttpResponse<String> send(HttpClient client,String address,String method,Map<String,String> headers,JsonNode data)throws Exception{
        return client.send(request(address,method,headers,data),HttpResponse.BodyHandlers.ofString());
    }
    private static JsonNode json(HttpResponse<String> response,int status)throws Exception{
        assertEquals(status,response.statusCode(),response.body());
        return Model.JSON.readTree(response.body());
    }
    private static void assertCode(HttpResponse<String> response,int status,String code)throws Exception{
        assertEquals(code,json(response,status).path("code").textValue());
    }
    private static String cookie(HttpResponse<String> response,String name){
        return response.headers().allValues("Set-Cookie").stream().filter(h->h.startsWith(name+"=")).findFirst().orElseThrow().split(";",2)[0];
    }
    private static String encoded(String value){return URLEncoder.encode(value,StandardCharsets.UTF_8);}
    private static Map<String,String> form(String encoded){
        return java.util.Arrays.stream(encoded.split("&"))
                .map(field->field.split("=",2))
                .collect(Collectors.toMap(pair->pair[0],pair->URLDecoder.decode(pair[1],StandardCharsets.UTF_8)));
    }

    private static final class Provider implements AutoCloseable {
        private final HttpsServer server;
        private final HttpClient client;
        private final String issuer,publicKey;
        private final PrivateKey signingKey;
        private final Map<String,Grant> grants=new ConcurrentHashMap<>();
        private final AtomicReference<Claim> next=new AtomicReference<>(Claim.VALID);
        private final AtomicInteger exchanges=new AtomicInteger();

        Provider(Path directory)throws Exception{
            Path keyFile=directory.resolve("fixture.p12");
            String pass="fixture-passphrase";
            Process keytool=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","keytool").toString(),
                    "-genkeypair","-alias","mock-oidc","-keyalg","RSA","-keysize","2048","-validity","1",
                    "-dname","CN=localhost","-ext","SAN=dns:localhost,ip:127.0.0.1","-storetype","PKCS12",
                    "-keystore",keyFile.toString(),"-storepass",pass,"-keypass",pass,"-noprompt")
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start();
            assertTrue(keytool.waitFor(10,TimeUnit.SECONDS));
            assertEquals(0,keytool.exitValue(),"Create the ephemeral TLS fixture key");
            KeyStore keys=KeyStore.getInstance("PKCS12");
            try(InputStream input=Files.newInputStream(keyFile)){keys.load(input,pass.toCharArray());}
            signingKey=(PrivateKey)keys.getKey("mock-oidc",pass.toCharArray());
            publicKey=Base64.getEncoder().encodeToString(keys.getCertificate("mock-oidc").getPublicKey().getEncoded());
            KeyManagerFactory km=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            km.init(keys,pass.toCharArray());
            SSLContext tls=SSLContext.getInstance("TLS");
            tls.init(km.getKeyManagers(),null,null);
            KeyStore trusted=KeyStore.getInstance("PKCS12");
            trusted.load(null,null);
            trusted.setCertificateEntry("mock-oidc",keys.getCertificate("mock-oidc"));
            TrustManagerFactory trust=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(trusted);
            SSLContext clientTls=SSLContext.getInstance("TLS");
            clientTls.init(null,trust.getTrustManagers(),null);
            client=HttpClient.newBuilder().sslContext(clientTls).followRedirects(HttpClient.Redirect.NEVER).build();
            server=HttpsServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"),0),8);
            server.setHttpsConfigurator(new HttpsConfigurator(tls));
            issuer="https://127.0.0.1:"+server.getAddress().getPort()+"/oidc";
            server.createContext("/oidc/authorize",this::authorize);
            server.createContext("/oidc/token",this::token);
            server.start();
        }

        BrowserAttempt begin(String entry,Claim claim,Map<String,String> headers)throws Exception{
            next.set(claim);
            HttpResponse<String> start=send(client,entry+OidcBrowserLogin.START,"GET",headers,null);
            assertEquals(302,start.statusCode(),start.body());
            String cookie=start.headers().firstValue("Set-Cookie").orElseThrow();
            assertTrue(cookie.contains("Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=300"));
            String authorization=start.headers().firstValue("Location").orElseThrow();
            assertFalse(authorization.contains("code_verifier"));
            assertTrue(authorization.startsWith(issuer+"/authorize?"));
            HttpResponse<String> authorize=send(client,authorization,"GET",Map.of(),null);
            assertEquals(302,authorize.statusCode(),authorize.body());
            URI destination=URI.create(authorize.headers().firstValue("Location").orElseThrow());
            assertTrue(destination.toString().startsWith(ORIGIN+OidcBrowserLogin.CALLBACK+"?"));
            String query=destination.getRawQuery(),code=form(query).get("code");
            return new BrowserAttempt(cookie.split(";",2)[0],query,grants.get(code).expiresAt,code);
        }
        HttpResponse<String> finish(BrowserAttempt attempt,String entry)throws Exception{
            return send(client,entry+OidcBrowserLogin.CALLBACK+"?"+attempt.query,"GET",Map.of("Cookie",attempt.cookie),null);
        }
        private void authorize(HttpExchange request){
            try(request){
                Map<String,String> query=form(request.getRequestURI().getRawQuery());
                if(!"GET".equals(request.getRequestMethod())||!CLIENT.equals(query.get("client_id"))
                        ||!"code".equals(query.get("response_type"))||!"openid".equals(query.get("scope"))
                        ||!"S256".equals(query.get("code_challenge_method"))
                        ||!(ORIGIN+OidcBrowserLogin.CALLBACK).equals(query.get("redirect_uri"))){
                    request.sendResponseHeaders(400,-1);return;
                }
                String code=UUID.randomUUID().toString();
                Claim claim=next.getAndSet(Claim.VALID);
                Instant expiry=Instant.ofEpochSecond(Instant.now().getEpochSecond()+(claim==Claim.EXPIRED?-5:120));
                grants.put(code,new Grant(query.get("nonce"),query.get("code_challenge"),claim,expiry));
                request.getResponseHeaders().set("Location",query.get("redirect_uri")+"?code="+code+"&state="+query.get("state")+"&iss="+encoded(issuer));
                request.sendResponseHeaders(302,-1);
            }catch(Exception unexpected){throw new IllegalStateException(unexpected);}
        }
        private void token(HttpExchange request){
            try(request){
                exchanges.incrementAndGet();
                Map<String,String> fields=form(new String(request.getRequestBody().readAllBytes(),StandardCharsets.UTF_8));
                Grant grant=grants.remove(fields.get("code"));
                String authorization="Basic "+Base64.getEncoder().encodeToString((encoded(CLIENT)+":"+encoded(SECRET)).getBytes(StandardCharsets.US_ASCII));
                String verifier=fields.get("code_verifier");
                if(!"POST".equals(request.getRequestMethod())||grant==null||!authorization.equals(request.getRequestHeaders().getFirst("Authorization"))
                        ||!"authorization_code".equals(fields.get("grant_type"))||!(ORIGIN+OidcBrowserLogin.CALLBACK).equals(fields.get("redirect_uri"))
                        ||verifier==null||!grant.challenge.equals(URL64.encodeToString(MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.US_ASCII))))){
                    request.sendResponseHeaders(400,-1);return;
                }
                byte[] token=Model.JSON.writeValueAsBytes(Map.of("id_token",signedToken(grant),"access_token","unused"));
                request.getResponseHeaders().set("Content-Type","application/json;charset=utf-8");
                request.sendResponseHeaders(200,token.length);
                request.getResponseBody().write(token);
            }catch(Exception unexpected){throw new IllegalStateException(unexpected);}
        }
        private String signedToken(Grant grant)throws Exception{
            String header=URL64.encodeToString(Model.JSON.writeValueAsBytes(Map.of("alg","RS256","kid",SIGNING_KEY_ID)));
            ObjectNode claims=Model.JSON.createObjectNode().put("iss",grant.claim==Claim.BAD_ISSUER?"https://foreign.example":issuer)
                    .put("sub",grant.claim==Claim.WRONG_SUBJECT?"another-user":SUBJECT)
                    .put("aud",grant.claim==Claim.WRONG_AUDIENCE?"another-client":CLIENT)
                    .put("nonce",grant.claim==Claim.BAD_NONCE?"incorrect":grant.nonce)
                    .put("iat",Instant.now().getEpochSecond()-10).put("exp",grant.expiresAt.getEpochSecond())
                    .put("role","ADMIN").put("email","legacy-admin@example.test");
            claims.putArray("groups").add("ADMIN");
            String content=header+"."+URL64.encodeToString(Model.JSON.writeValueAsBytes(claims));
            Signature signer=Signature.getInstance("SHA256withRSA");
            signer.initSign(signingKey);
            signer.update(content.getBytes(StandardCharsets.US_ASCII));
            byte[] signature=signer.sign();
            if(grant.claim==Claim.BAD_SIGNATURE)signature[0]^=1;
            return content+"."+URL64.encodeToString(signature);
        }
        @Override public void close(){server.stop(0);}
    }
}

package io.github.rivomesh.cdc;

import com.sun.net.httpserver.*;
import io.github.rivomesh.SyncFailure;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DorisHttpClientTest {
    interface Handler {void handle(HttpExchange exchange)throws Exception;}
    static HttpServer server(Handler handler)throws Exception{HttpServer s=HttpServer.create(new InetSocketAddress("127.0.0.1",0),8);s.createContext("/",exchange->{try{try(java.io.InputStream input=exchange.getRequestBody()){byte[] buffer=new byte[1024];while(input.read(buffer)!=-1){}}handler.handle(exchange);}catch(Throwable failure){System.err.println("DORIS_HTTP_FIXTURE_FAILURE "+failure.getClass().getName()+": "+failure.getMessage());throw new java.io.IOException(failure);}finally{exchange.close();}});s.start();return s;}
    static String url(HttpServer s){return "http://127.0.0.1:"+s.getAddress().getPort();}
    static DorisHttpClient.Settings settings(HttpServer s){DorisHttpClient.Settings config=new DorisHttpClient.Settings();config.url=url(s);config.username="sync";config.password="private-password";return config;}
    static String success(){return "{\"Status\":\"Success\",\"Label\":\"fixed_label\",\"NumberTotalRows\":1,\"NumberLoadedRows\":1,\"NumberFilteredRows\":0,\"NumberUnselectedRows\":0}";}
    static void respond(HttpExchange exchange,int status,String value)throws Exception{byte[] bytes=value.getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(status,bytes.length);exchange.getResponseBody().write(bytes);}
    static void load(DorisHttpClient client)throws Exception{client.load("business","physical","fixed_label","[{\"n\":1}]".getBytes(StandardCharsets.UTF_8),1);}
    @Test void strictNativeHeadersAndExactRowsAreRequired()throws Exception{
        AtomicReference<String> received=new AtomicReference<>();HttpServer s=server(exchange->{received.set(exchange.getRequestMethod());assertEquals("true",exchange.getRequestHeaders().getFirst("num_as_string"));assertEquals("false",exchange.getRequestHeaders().getFirst("read_json_by_line"));assertEquals("true",exchange.getRequestHeaders().getFirst("strict_mode"));assertEquals("0",exchange.getRequestHeaders().getFirst("max_filter_ratio"));assertEquals("Basic "+Base64.getEncoder().encodeToString("sync:private-password".getBytes(StandardCharsets.UTF_8)),exchange.getRequestHeaders().getFirst("Authorization"));respond(exchange,200,success());});try(DorisHttpClient client=new DorisHttpClient(settings(s))){load(client);assertEquals("PUT",received.get());}finally{s.stop(0);}
    }
    @Test void droppedRowsDuplicateJsonAndWrongLabelsNeverConfirm()throws Exception{
        for(String response:Arrays.asList(success().replace("\"NumberLoadedRows\":1","\"NumberLoadedRows\":0"),success().replace("fixed_label","wrong_label"),success().replace("\"Status\":","\"Status\":\"Fail\",\"Status\":"))){HttpServer s=server(exchange->respond(exchange,200,response));try(DorisHttpClient client=new DorisHttpClient(settings(s))){SyncFailure error=assertThrows(SyncFailure.class,()->load(client));assertFalse(error.retryable);}finally{s.stop(0);}}
    }
    @Test void publishTimeoutAndPendingLabelsRetryButOnlyFinishedLabelsConfirm()throws Exception{
        for(String state:Arrays.asList("RUNNING","FINISHED","CANCELLED")){HttpServer s=server(exchange->respond(exchange,200,"{\"Status\":\"Label Already Exists\",\"Label\":\"fixed_label\",\"ExistingJobStatus\":\""+state+"\"}"));try(DorisHttpClient client=new DorisHttpClient(settings(s))){if(state.equals("FINISHED"))load(client);else assertEquals(state.equals("RUNNING"),assertThrows(SyncFailure.class,()->load(client)).retryable);}finally{s.stop(0);}}
        HttpServer s=server(exchange->respond(exchange,200,"{\"Status\":\"Publish Timeout\",\"Label\":\"fixed_label\"}"));try(DorisHttpClient client=new DorisHttpClient(settings(s))){assertTrue(assertThrows(SyncFailure.class,()->load(client)).retryable);}finally{s.stop(0);}
    }
    @Test void credentialsFollowOnlyAnExplicitSamePathRedirect()throws Exception{
        AtomicInteger targetCalls=new AtomicInteger(),sourceCalls=new AtomicInteger();HttpServer target=server(exchange->{targetCalls.incrementAndGet();assertNotNull(exchange.getRequestHeaders().getFirst("Authorization"));respond(exchange,200,success());});HttpServer source=server(exchange->{sourceCalls.incrementAndGet();exchange.getResponseHeaders().set("Location",url(target)+exchange.getRequestURI().getRawPath());exchange.sendResponseHeaders(307,-1);});
        try{try(DorisHttpClient client=new DorisHttpClient(settings(source))){assertFalse(assertThrows(SyncFailure.class,()->load(client)).retryable);}assertEquals(0,targetCalls.get());DorisHttpClient.Settings allowed=settings(source);allowed.redirectOrigins=Collections.singletonList(url(target));try(DorisHttpClient client=new DorisHttpClient(allowed)){try{load(client);}catch(Exception failure){throw new AssertionError("Approved redirect failed; source calls="+sourceCalls.get()+"; target calls="+targetCalls.get(),failure);}}assertEquals(1,targetCalls.get());}finally{source.stop(0);target.stop(0);}
    }
    @Test void networkErrorsAreSafeAndRetriableWhileBadOriginsAreRejected()throws Exception{
        DorisHttpClient.Settings config=new DorisHttpClient.Settings();config.url="http://127.0.0.1:9";config.username="sync";config.password="private-password";try(DorisHttpClient client=new DorisHttpClient(config)){SyncFailure failure=assertThrows(SyncFailure.class,()->load(client));assertTrue(failure.retryable);assertEquals("DORIS_CONNECTION_RETRY",failure.code);assertFalse(failure.getMessage().contains(config.password));}config.url="https://example.test";config.redirectOrigins=Collections.singletonList("http://example.test");assertThrows(IllegalArgumentException.class,()->new DorisHttpClient(config));config.redirectOrigins=Collections.emptyList();config.url="http://name:password@example.test";assertThrows(IllegalArgumentException.class,()->new DorisHttpClient(config));
    }
}

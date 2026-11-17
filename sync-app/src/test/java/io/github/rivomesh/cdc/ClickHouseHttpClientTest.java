package io.github.rivomesh.cdc;

import com.sun.net.httpserver.*;
import io.github.rivomesh.SyncFailure;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ClickHouseHttpClientTest {
    interface Handler{void handle(HttpExchange exchange)throws Exception;}
    void server(Handler handler,Handler test)throws Exception{
        HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),8);
        server.createContext("/",exchange->{try{handler.handle(exchange);}catch(Exception failure){throw new java.io.IOException(failure);}finally{exchange.close();}});server.start();
        try{current="http://127.0.0.1:"+server.getAddress().getPort();test.handle(null);}finally{server.stop(0);}
    }
    String current;
    ClickHouseHttpClient client()throws Exception{ClickHouseHttpClient.Settings s=new ClickHouseHttpClient.Settings();s.url=current;s.username="sync";s.password="private-password";return new ClickHouseHttpClient(s);}
    static void reply(HttpExchange exchange,int status,String body)throws Exception{byte[] bytes=body.getBytes(StandardCharsets.UTF_8);exchange.sendResponseHeaders(status,bytes.length==0?-1:bytes.length);if(bytes.length>0)exchange.getResponseBody().write(bytes);}
    @Test void postsCredentialsInHeadersAndRequiresCompleteJson()throws Exception{
        server(exchange->{assertEquals("POST",exchange.getRequestMethod());assertFalse(exchange.getRequestURI().toString().contains("private"));assertTrue(exchange.getRequestURI().getQuery().contains("wait_end_of_query=1"));assertEquals("Basic "+Base64.getEncoder().encodeToString("sync:private-password".getBytes(StandardCharsets.UTF_8)),exchange.getRequestHeaders().getFirst("Authorization"));reply(exchange,200,"{\"meta\":[{\"name\":\"n\",\"type\":\"UInt64\"}],\"data\":[[\"18446744073709551615\"]],\"rows\":1}");},ignored->{try(ClickHouseHttpClient c=client()){assertEquals("18446744073709551615",c.query("SELECT 1").path("data").get(0).get(0).asText());}});
    }
    @Test void http200WithAppendedExceptionCannotBecomeAnAcknowledgement()throws Exception{
        server(exchange->reply(exchange,200,"{\"meta\":[],\"data\":[],\"rows\":0}\nCode: 395. DB::Exception: secret row"),ignored->{try(ClickHouseHttpClient c=client()){SyncFailure f=assertThrows(SyncFailure.class,()->c.query("SELECT 1"));assertEquals("CLICKHOUSE_RESPONSE_INVALID",f.code);assertFalse(f.getMessage().contains("secret"));}});
        server(exchange->reply(exchange,200,"Code: 395. DB::Exception: secret row"),ignored->{try(ClickHouseHttpClient c=client()){assertEquals("CLICKHOUSE_UNEXPECTED_COMMAND_RESPONSE",assertThrows(SyncFailure.class,()->c.execute("INSERT INTO t VALUES (1)")).code);}});
    }
    @Test void neverFollowsRedirectOrRetriesAmbiguousWrites()throws Exception{
        AtomicInteger requests=new AtomicInteger();
        server(exchange->{requests.incrementAndGet();exchange.getResponseHeaders().set("Location",current+"/elsewhere");reply(exchange,307,"");},ignored->{try(ClickHouseHttpClient c=client()){assertEquals("CLICKHOUSE_REDIRECT_REQUIRES_PLAN",assertThrows(SyncFailure.class,()->c.execute("INSERT INTO t VALUES (1)")).code);assertEquals(1,requests.get());}});
    }
    @Test void exceptionHeaderAndOversizedBodyFailWithoutLeakingContents()throws Exception{
        server(exchange->{exchange.getResponseHeaders().set("X-ClickHouse-Exception-Code","241");reply(exchange,500,"secret SQL and credentials");},ignored->{try(ClickHouseHttpClient c=client()){SyncFailure f=assertThrows(SyncFailure.class,()->c.execute("INSERT INTO t VALUES (1)"));assertEquals("CLICKHOUSE_SERVER_241",f.code);assertFalse(f.getMessage().contains("secret"));}});
        server(exchange->reply(exchange,200,String.join("",Collections.nCopies(2048,"x"))),ignored->{ClickHouseHttpClient.Settings s=new ClickHouseHttpClient.Settings();s.url=current;s.username="sync";s.password="private";s.maximumResponseBytes=1024;try(ClickHouseHttpClient c=new ClickHouseHttpClient(s)){assertEquals("CLICKHOUSE_RESPONSE_LIMIT",assertThrows(SyncFailure.class,()->c.execute("SELECT 1")).code);}});
    }
    @Test void connectionLossAfterReceivingTheBodyDoesNotReplayTheWrite()throws Exception{
        AtomicInteger requests=new AtomicInteger();
        server(exchange->{requests.incrementAndGet();byte[] buffer=new byte[256];while(exchange.getRequestBody().read(buffer)!=-1){/* receive the entire write, then lose its response */}},ignored->{try(ClickHouseHttpClient c=client()){SyncFailure f=assertThrows(SyncFailure.class,()->c.execute("INSERT INTO t VALUES (1)"));assertEquals("CLICKHOUSE_CONNECTION_RETRY",f.code);assertTrue(f.retryable);assertEquals(1,requests.get(),"HTTP transport replayed an ambiguous write");}});
    }
}

package io.github.rivomesh.cdc;

import com.sun.net.httpserver.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import io.github.rivomesh.transport.classic.ClassicTransport;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ClassicAdminClientTest {
    ClassicAdminClient client(HttpServer server)throws Exception{return new ClassicAdminClient("http://127.0.0.1:"+server.getAddress().getPort()+"/api/jolokia/","admin","private-password","broker",500,1000);}
    HttpServer server(HttpHandler handler)throws Exception{HttpServer s=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);s.createContext("/",handler);s.start();return s;}
    @Test void absenceRequiresAnExplicitSuccessfulEmptySearch()throws Exception{
        HttpServer s=server(x->{byte[] body="{\"status\":200,\"value\":[]}".getBytes(StandardCharsets.UTF_8);x.sendResponseHeaders(200,body.length);x.getResponseBody().write(body);x.close();});try{assertFalse(client(s).exists("rivomesh.queue"));}finally{s.stop(0);}
        HttpServer invalid=server(x->{byte[] body="not a Jolokia endpoint".getBytes(StandardCharsets.UTF_8);x.sendResponseHeaders(404,body.length);x.getResponseBody().write(body);x.close();});try{assertEquals("CLASSIC_ADMIN_RESPONSE_INVALID",assertThrows(ClassicTransport.Failure.class,()->clientHolder(invalid)).code);}finally{invalid.stop(0);}
    }
    private void clientHolder(HttpServer server)throws Exception{client(server).exists("rivomesh.queue");}
    @Test void redirectsAndHttp200JmxErrorsCannotBecomeSuccess()throws Exception{
        AtomicInteger calls=new AtomicInteger();HttpServer s=server(x->{calls.incrementAndGet();x.getResponseHeaders().add("Location","/credentials-leak");x.sendResponseHeaders(307,-1);x.close();});try{assertEquals("CLASSIC_ADMIN_REDIRECT_REJECTED",assertThrows(ClassicTransport.Failure.class,()->clientHolder(s)).code);assertEquals(1,calls.get());}finally{s.stop(0);}
        HttpServer error=server(x->{byte[] body="{\"status\":500,\"error\":\"sensitive server detail\"}".getBytes(StandardCharsets.UTF_8);x.sendResponseHeaders(200,body.length);x.getResponseBody().write(body);x.close();});try{ClassicTransport.Failure f=assertThrows(ClassicTransport.Failure.class,()->clientHolder(error));assertEquals("CLASSIC_ADMIN_OPERATION_REJECTED",f.code);assertFalse(f.getMessage().contains("sensitive"));}finally{error.stop(0);}
    }
    @Test void credentialsStayInHeadersAndDuplicateJsonKeysAreRejected()throws Exception{
        HttpServer s=server(x->{assertNull(x.getRequestURI().getQuery());assertEquals("http://127.0.0.1:"+x.getLocalAddress().getPort()+"/",x.getRequestHeaders().getFirst("Referer"));assertTrue(x.getRequestHeaders().getFirst("Authorization").startsWith("Basic "));byte[] body="{\"status\":200,\"value\":{\"QueueSize\":0},\"status\":404}".getBytes(StandardCharsets.UTF_8);x.sendResponseHeaders(200,body.length);x.getResponseBody().write(body);x.close();});try{assertEquals("CLASSIC_ADMIN_RESPONSE_INVALID",assertThrows(ClassicTransport.Failure.class,()->clientHolder(s)).code);}finally{s.stop(0);}
    }
    @Test void subscriptionsUseTheActualSelectorMbeanWithoutAnInventedBrowserAttribute()throws Exception{
        HttpServer s=server(x->{byte[] request=new byte[4096];int read=x.getRequestBody().read(request);String body=new String(request,0,read,StandardCharsets.UTF_8);assertFalse(body.contains("Browser"));String value=body.contains("Subscriptions")?"{\"Subscriptions\":[\"org.apache.activemq:type=Broker,brokerName=broker,endpoint=Consumer,consumerId=1\"]}":"{\"Selector\":\"JMSPriority = 9\"}";byte[] response=("{\"status\":200,\"value\":"+value+"}").getBytes(StandardCharsets.UTF_8);x.sendResponseHeaders(200,response.length);x.getResponseBody().write(response);x.close();});try{assertEquals("CLASSIC_CONSUMER_SELECTOR_REQUIRED",assertThrows(ClassicTransport.Failure.class,()->client(s).consumers("rivomesh.events","rivomesh.events.receipts")).code);}finally{s.stop(0);}
    }

}

package io.github.rivomesh.transport.classic;

import javax.jms.*;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ClassicTransportTest {
    @Test void linkedTlsAndAuthenticationFaultsDoNotBecomeRetryableConnections(){
        JMSException linked=new JMSException("wrapper");linked.setLinkedException(new SSLHandshakeException("synthetic"));ClassicTransport.Failure tls=(ClassicTransport.Failure)ClassicTransport.classify(linked);assertEquals("CLASSIC_TLS_REJECTED",tls.code);assertFalse(tls.retryable);
        JMSException auth=new JMSException("wrapper");auth.setLinkedException(new JMSSecurityException("synthetic"));ClassicTransport.Failure rejected=(ClassicTransport.Failure)ClassicTransport.classify(auth);assertEquals("CLASSIC_AUTHORIZATION_REJECTED",rejected.code);assertFalse(rejected.retryable);
    }
    @Test void formatAndDestinationErrorsPauseWhileTransportAndCapacityCanRetry(){
        for(JMSException error:new JMSException[]{new InvalidSelectorException("bad"),new InvalidDestinationException("bad"),new MessageFormatException("bad")})assertFalse(((ClassicTransport.Failure)ClassicTransport.classify(error)).retryable);
        assertTrue(((ClassicTransport.Failure)ClassicTransport.classify(new ResourceAllocationException("full"))).retryable);assertTrue(((ClassicTransport.Failure)ClassicTransport.classify(new JMSException("lost"))).retryable);
    }
}

package io.github.rivomesh.cdc;

import io.nats.client.impl.SocketDataPort;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/** Connects JetStream TLS with the JVM trust store and verifies the configured entrance name. */
public final class VerifiedNatsTlsPort extends SocketDataPort {
    @Override
    public void upgradeToSecure() throws IOException {
        SSLSocket secured;
        try {
            secured = (SSLSocket) SSLContext.getDefault().getSocketFactory().createSocket(socket, host, port, true);
        } catch (NoSuchAlgorithmException error) {
            throw new IOException("The JVM default TLS context is unavailable", error);
        }
        try {
            SSLParameters parameters = secured.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            secured.setUseClientMode(true);
            secured.setSSLParameters(parameters);
            secured.startHandshake();
            in = secured.getInputStream();
            out = secured.getOutputStream();
            socket = secured;
            isSecure = true;
        } catch (IOException | RuntimeException error) {
            try {
                secured.close();
            } catch (IOException closeError) {
                error.addSuppressed(closeError);
            }
            throw error;
        }
    }
}

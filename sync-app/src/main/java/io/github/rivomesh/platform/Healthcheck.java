package io.github.rivomesh.platform;

import java.net.*;

/** Container healthcheck without curl/wget dependencies or database side effects. */
public final class Healthcheck {
    public static void main(String[] args) {
        try {
            HttpURLConnection c=(HttpURLConnection)new URL(args.length==0?"http://127.0.0.1:8080/healthz":args[0]).openConnection();
            c.setConnectTimeout(2000);c.setReadTimeout(2000);int status=c.getResponseCode();c.disconnect();if(status!=200)System.exit(1);
        }catch(Exception e){System.exit(1);}
    }
}

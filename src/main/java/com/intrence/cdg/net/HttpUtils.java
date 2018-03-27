package com.intrence.cdg.net;


import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class HttpUtils {
    private static final Logger LOGGER = Logger.getLogger(HttpUtils.class);

    private CustomSSLConnectionSocketFactory customSslConnectionSocketFactory = null;

    private static class CustomSSLConnectionSocketFactory extends SSLConnectionSocketFactory {
        private CustomSSLConnectionSocketFactory(SSLContext sslContext) {
            // ignore host name verification or we can't access sites like https://www.selectsitter.com
            super(sslContext);
        }

        @Override
        protected void prepareSocket(SSLSocket socket) throws IOException {
            super.prepareSocket(socket);

            // Some sites (i.e. https://www.newunivera.com) only allow SSLv3 so this
            // fixes the error "bad_record_mac"
            // http://mail-archives.apache.org/mod_mbox/hc-httpclient-users/200902.mbox/%3CCE1A701EDAB9A447B24DD8045832F67E4C97D89302@FRSPX100.fr01.awl.atosorigin.net%3E
            socket.setEnabledProtocols(new String[]{"SSLv3"});
        }
    }

    public static CustomSSLConnectionSocketFactory getCustomSslConnectionSocketFactory() {
        return new CustomSSLConnectionSocketFactory(getSslContext());

    }

    // In order to fetch certified pages, we ignore SSL certs because we aren't worried about MITM attacks
    // Some sites you can test on: https://www.alutiiq.com, https://www.mycouponriot.com
    // http://stackoverflow.com/questions/6047996/ignore-self-signed-ssl-cert-using-jersey-client

    // Good discussion between Mark and Taylor here: https://github.groupondev.com/voltron/khan-proxy-service/pull/39
    private static  SSLContext getSslContext() {
        SSLContext sslContext = null;

        // Create a trust manager that does not validate certificate chains
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }
        }};

        // Install the all-trusting trust manager
        try {
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());
        } catch (Exception e) {
            // ignore
        }
        return sslContext;
    }



    public static class IdleConnectionMonitorThread implements Runnable {
        private final PoolingHttpClientConnectionManager connMgr;
        private volatile boolean shutdown;
        public IdleConnectionMonitorThread(PoolingHttpClientConnectionManager connMgr) {
            this.connMgr = connMgr;
        }

        @Override
        public void run() {
            if(!shutdown) {
                int before = 0;
                if(connMgr.getRoutes() != null)
                     before = connMgr.getRoutes().size();
                connMgr.closeExpiredConnections();
                connMgr.closeIdleConnections(5, TimeUnit.MINUTES);
                Set<HttpRoute> routes = connMgr.getRoutes();
                if(routes != null) {
                    for (HttpRoute route : routes) {
                        LOGGER.info(String.format("stats for route=%s are %s", route.toString(),
                                connMgr.getStats(route)));
                    }

                    LOGGER.info(String.format("Total routes before=%d, after=%s and stats=%s", before,
                            routes.size(), connMgr.getTotalStats()));
                }else{
                    LOGGER.info("No HttpRoutes available in PoolingHttpClientConnectionManager");
                }
            } else {
                LOGGER.info("PoolingHttpClientConnectionManager is getting shutdown");
            }
        }

        public void shutdown() {
            shutdown = true;
            synchronized (this) {
                notifyAll();
            }
        }
    }

}

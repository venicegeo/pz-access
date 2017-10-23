/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package access;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.cookie.Cookie;
import org.apache.http.cookie.CookieOrigin;
import org.apache.http.cookie.CookieSpec;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.cookie.MalformedCookieException;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.cookie.DefaultCookieSpec;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.client.RestTemplate;

import access.deploy.geoserver.AuthHeaders;
import access.deploy.geoserver.BasicAuthHeaders;
import access.deploy.geoserver.PKIAuthHeaders;


@SpringBootApplication
@Configuration
@EnableAsync
@EnableScheduling
@EnableAutoConfiguration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = { "org.venice.piazza.common.hibernate" })
@EntityScan(basePackages = { "org.venice.piazza.common.hibernate" })
@ComponentScan(basePackages = { "access", "util", "org.venice.piazza" })
public class Application extends SpringBootServletInitializer implements AsyncConfigurer {
	@Value("${thread.count.size}")
	private int threadCountSize;
	@Value("${thread.count.limit}")
	private int threadCountLimit;

	private static final Logger LOG = LoggerFactory.getLogger(Application.class);

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		return builder.sources(Application.class);
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args); // NOSONAR
	}

	@Override
	@Bean
	public Executor getAsyncExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(threadCountSize);
		executor.setMaxPoolSize(threadCountLimit);
		executor.initialize();
		return executor;
	}

	@Override
	public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
		return (ex, method, params) -> LOG.error("Uncaught Threading exception encountered in {} with details: {}", ex.getMessage(), method.getName());
	}
	
	@Configuration
	@Profile({ "basic-geoserver-auth" })
	protected static class BasicAuthenticationConfig {

		@Value("${http.max.total}")
		private int httpMaxTotal;

		@Value("${http.max.route}")
		private int httpMaxRoute;

		@Bean
		public RestTemplate restTemplate() {			
			final RestTemplate restTemplate = new RestTemplate();
			final HttpClient httpClient = HttpClientBuilder.create().setMaxConnTotal(httpMaxTotal).setMaxConnPerRoute(httpMaxRoute).build();
			restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory(httpClient));
			return restTemplate;
		}
		
		@Bean
		public AuthHeaders authHeaders() {
			return new BasicAuthHeaders();
		}
	}

	@Configuration
	@Profile({ "pki-geoserver-auth" })
	protected static class PKIAuthenticationConfig {

		@Value("${http.max.total}")
		private int httpMaxTotal;

		@Value("${http.max.route}")
		private int httpMaxRoute;

		@Value("${JKS_FILE}")
		private String keystoreFileName;

		@Value("${JKS_PASSPHRASE}")
		private String keystorePassphrase;

		@Value("${PZ_PASSPHRASE}")
		private String piazzaKeyPassphrase;

		@Bean
		public RestTemplate restTemplate() throws KeyManagementException, UnrecoverableKeyException, NoSuchAlgorithmException,
				KeyStoreException, CertificateException, IOException {
			
			SSLContext sslContext = SSLContexts.custom()
					.loadKeyMaterial(getStore(), piazzaKeyPassphrase.toCharArray())
					.loadTrustMaterial(null, new TrustSelfSignedStrategy()).useProtocol("TLS").build();

			Registry<CookieSpecProvider> registry = RegistryBuilder.<CookieSpecProvider>create().register("myspec", new MySpecProvider()).build();
			RequestConfig requestConfig = RequestConfig.custom().setCookieSpec("myspec").setCircularRedirectsAllowed(true).build();
			
			HttpClient httpClient = HttpClientBuilder.create()
					.setDefaultRequestConfig(requestConfig)
					.setMaxConnTotal(httpMaxTotal)
					.setSSLContext(sslContext)
					.setSSLHostnameVerifier(new NoopHostnameVerifier())
					.setDefaultCookieStore(new BasicCookieStore())
					.setDefaultCookieSpecRegistry(registry)
					.setRedirectStrategy(new MyRedirectStrategy())
					.setMaxConnPerRoute(httpMaxRoute).build();
			
			RestTemplate restTemplate = new RestTemplate();
			restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory(httpClient));
			
			List<HttpMessageConverter<?>> messageConverters = new ArrayList<HttpMessageConverter<?>>();
			messageConverters.add(new StringHttpMessageConverter());
			messageConverters.add(new MappingJackson2HttpMessageConverter());
			restTemplate.setMessageConverters(messageConverters);
			
			return restTemplate;
		}

		@Bean
		public AuthHeaders authHeaders() {
			return new PKIAuthHeaders();
		}
		
		protected KeyStore getStore() throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
			final KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
			InputStream inputStream = getClass().getClassLoader().getResourceAsStream(keystoreFileName);
			try {
				store.load(inputStream, keystorePassphrase.toCharArray());
			} finally {
				inputStream.close();
			}

			return store;
		}

		protected class MyCookieSpec extends DefaultCookieSpec {
			
			@Override
			public void validate(Cookie c, CookieOrigin co) throws MalformedCookieException {
				// Do nothing; accept all cookies
			}
		}
		
		protected class MySpecProvider implements CookieSpecProvider {
			
			@Override
			public CookieSpec create(HttpContext context) {
				
				return new MyCookieSpec();
			}
		}
		
		protected class MyRedirectStrategy extends DefaultRedirectStrategy {
			
		    @Override
		    protected boolean isRedirectable(final String method) {

		        return true;
		    }
		    
		    @Override
		    public HttpUriRequest getRedirect(final HttpRequest request, final HttpResponse response, 
		    		final HttpContext context) throws ProtocolException {
		    	
		        final URI uri = getLocationURI(request, response, context);
		        final String method = request.getRequestLine().getMethod();
		        
		        if (method.equalsIgnoreCase(HttpHead.METHOD_NAME)) {
		            return new HttpHead(uri);
		        } 
		        else if (method.equalsIgnoreCase(HttpGet.METHOD_NAME)) {
		            return new HttpGet(uri);
		        } 
		        else {
		            final int status = response.getStatusLine().getStatusCode();
		            
		            if (status == HttpStatus.SC_TEMPORARY_REDIRECT || status ==  HttpStatus.SC_MOVED_PERMANENTLY 
		            		|| status == HttpStatus.SC_MOVED_TEMPORARILY) {
		                return RequestBuilder.copy(request).setUri(uri).build();
		            } 
		            else {
		                return new HttpGet(uri);
		            }
		        }
		    }
		}
	}
}
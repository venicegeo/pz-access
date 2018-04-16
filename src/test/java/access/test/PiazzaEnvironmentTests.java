/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package access.test;

import access.deploy.geoserver.AuthHeaders;
import access.deploy.geoserver.PiazzaEnvironment;
import access.util.AccessUtilities;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import util.PiazzaLogger;

import static org.junit.Assert.assertTrue;

/**
 * Tests the Database Accessor CRUD Utility
 *
 * @author Sonny.Saniev
 */
public class PiazzaEnvironmentTests {

    @Mock
    private AccessUtilities accessUtilities;
    @Mock
    private RestTemplate restTemplate;
    @Mock
    private AuthHeaders authHeaders;
    @Mock
    private PiazzaLogger pzLogger;
    @InjectMocks
    private PiazzaEnvironment piazzaEnvironment;

    private HttpClient httpClient;

    /**
     * Test initialization
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.httpClient = HttpClientBuilder.create().build();
        ReflectionTestUtils.setField(this.piazzaEnvironment, "httpClient", this.httpClient);
        ReflectionTestUtils.setField(this.piazzaEnvironment, "restTemplateConnectionReadTimeout", 2000);

        ReflectionTestUtils.setField(this.piazzaEnvironment, "postgresServiceKeyUser", "junit_user");
        ReflectionTestUtils.setField(this.piazzaEnvironment, "postgresServiceKeyPassword", "junit_user_password");
        ReflectionTestUtils.setField(this.piazzaEnvironment, "postgresPort", "5432");
        ReflectionTestUtils.setField(this.piazzaEnvironment, "postgresDatabase", "junit_database_name");
        ReflectionTestUtils.setField(this.piazzaEnvironment, "postgresHost", "junit_host");

        Mockito.when(this.accessUtilities.getGeoServerBaseUrl()).thenReturn("http://localhost:8080/geoserver");
    }

    @Test
    public void testResourcesExist() {
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/workspaces/piazza.json"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenReturn(new ResponseEntity<>("{\"result\": \"totally exists\"}", HttpStatus.OK));

        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/datastores/piazza.json"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenReturn(new ResponseEntity<>("{\"result\": \"totally exists\"}", HttpStatus.OK));

        piazzaEnvironment.initializeEnvironment();
        assertTrue(true); // no error occurred
    }

    @Test
    public void testResourcesDoNotExist() {
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/workspaces/piazza.json"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.NOT_FOUND));
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/workspaces"),
                Mockito.eq(HttpMethod.POST),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenReturn(new ResponseEntity<String>("{\"result\": \"all good\"}", HttpStatus.OK));
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/workspaces/piazza.xml"),
                Mockito.eq(HttpMethod.PUT),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenReturn(new ResponseEntity<String>("{\"result\": \"all good\"}", HttpStatus.OK));

        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/datastores/piazza.json"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenReturn(new ResponseEntity<String>("{invalid JSON}", HttpStatus.OK));
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/datastores"),
                Mockito.eq(HttpMethod.POST),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenReturn(new ResponseEntity<String>("{\"result\": \"all good\"}", HttpStatus.OK));

        piazzaEnvironment.initializeEnvironment();
        assertTrue(true); // no error occurred
    }

    @Test
    public void testResourcesCauseServerError() {
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/workspaces/piazza.json"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR));

        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/datastores/piazza.json"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR));

        piazzaEnvironment.initializeEnvironment();
        assertTrue(true); // no error occurred
    }

    @Test
    public void testResourcesCauseRestException() {
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/workspaces/piazza.json"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenThrow(new RestClientException("Error with the REST."));

        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/datastores/piazza.json"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenThrow(new RestClientException("Error with the REST."));

        piazzaEnvironment.initializeEnvironment();
        assertTrue(true); // no error occurred
    }

    @Test
    public void testResourceCreationServerError() {
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/workspaces/piazza.json"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.NOT_FOUND));
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/workspaces"),
                Mockito.eq(HttpMethod.POST),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenReturn(new ResponseEntity<>("{\"result\": \"all good\"}", HttpStatus.OK))
                .thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR))
                .thenThrow(new RuntimeException());
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/workspaces/piazza.xml"),
                Mockito.eq(HttpMethod.PUT),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR));

        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/datastores/piazza.json"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.NOT_FOUND));
        Mockito.when(this.restTemplate.exchange(
                Mockito.endsWith("/datastores"),
                Mockito.eq(HttpMethod.POST),
                Mockito.any(),
                Mockito.eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR))
                .thenThrow(new RuntimeException());

        this.piazzaEnvironment.initializeEnvironment();
        this.piazzaEnvironment.initializeEnvironment();
        this.piazzaEnvironment.initializeEnvironment();
    }
}

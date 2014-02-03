package edu.bc.casinepe;

import javax.print.attribute.standard.Media;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.glassfish.grizzly.http.server.HttpServer;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MoviesResourceTest {

    private HttpServer server;
    private WebTarget target;

    @Before
    public void setUp() throws Exception {
        // start the server
        server = Main.startServer();
        // create the client
        final Client client = ClientBuilder.newBuilder()
                .register(JacksonFeature.class)
                .build();

        // uncomment the following line if you want to enable
        // support for JSON in the client (you also have to uncomment
        // dependency on jersey-media-json module in pom.xml and Main.startServer())
        // --
        //c.configuration().enable(new org.glassfish.jersey.media.json.JsonJaxbFeature());

        target = client.target(Main.BASE_URI);
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void testRatingsFileExists() {
        assertNotNull("Ratings test file is missing", getClass().getResource("/mahoutRatings.dat"));
    }
    /**
     * Test to see that the message "Got it!" is sent in the response.
     */
    @Test
    public void testGetMovies() {
        String responseMsg = target.path("movies").request().get(String.class);
        //assertEquals("Got it!", responseMsg);
    }
}

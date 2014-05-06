package edu.bc.casinepe;

import edu.bc.casinepe.core.FileRatings;
import edu.bc.casinepe.core.RecommenderFactory;
import edu.bc.casinepe.jdbc.JDBCDataModel;
import edu.bc.casinepe.core.SimilarityFactory;
import edu.bc.casinepe.metrics.MetricSystem;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URI;

/**
 * Main class.
 *
 */
public class Main {
    // Base URI the Grizzly HTTP server will listen on
    //public static final String BASE_URI = "http://0.0.0.0:8080/";
    public static final String BASE_URI = "http://localhost:8080/";



    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * @return Grizzly HTTP server.
     */
    public static HttpServer startServer() {
        // create a resource config that scans for JAX-RS resources and providers
        // in edu.bc.casinepe package

        final ResourceConfig rc = new ResourceConfig().packages("edu.bc.casinepe.resources")
                                                      .register(JacksonFeature.class)
                                                      .register(CorsResponseFilter.class);
        MetricSystem.start();

        //Get resources into memory
        FileRatings.getRatingsFile();
        JDBCDataModel.getDataModel();

        /*RecommenderFactory.getUserUserLogCFRS();
        RecommenderFactory.getUserUserPearsonCFRS();*/

        RecommenderFactory.getItemItemLogCFRS();
        RecommenderFactory.getItemItemPearsonCFRS();
        RecommenderFactory.getItemAverageRecommender();


        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
    }

    /**
     * Main method.
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        final HttpServer server = startServer();
        System.out.println(String.format("Jersey app started with WADL available at "
                + "%sapplication.wadl\nHit enter to stop it...", BASE_URI));
        System.in.read();
        server.stop();
    }
}


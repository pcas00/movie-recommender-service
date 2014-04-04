package edu.bc.casinepe.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;

import org.apache.logging.log4j.LogManager;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * references a MetricRegistry that is used to instrument the code base.
 */
public class MetricSystem {

    private static org.apache.logging.log4j.Logger logger = LogManager.getLogger(MetricSystem.class.getName());
    private static boolean started = false;
    public static final MetricRegistry metrics = new MetricRegistry();

    private static final Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
            .outputTo(LoggerFactory.getLogger(MetricSystem.class))
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();


    public static void start() {
        if (!started) {
            reporter.start(1, TimeUnit.SECONDS);
            started = true;
            logger.info("MetricSystem has been started.");
        }
    }

    public static void stop() {
        reporter.stop();
        logger.info("MetricSystem has been stopped.");

    }

}

package edu.bc.casinepe;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * references a MetricRegistry that is used to instrument the code base.
 */
public class MetricSystem {

    private static final Logger logger = LoggerFactory.getLogger(MetricSystem.class);
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

package io.github.manto.starter;

/**
 * Marker class for {@code manto-spring-boot-starter}.
 *
 * <p>The starter itself contains no code; it aggregates
 * {@code manto-spring-boot-autoconfigure}, {@code manto-kafka} and
 * {@code manto-core}. This class exists solely to provide a non-empty
 * source and Javadoc JAR for Maven Central publication (jar packaging
 * requires {@code -sources.jar} and {@code -javadoc.jar}).
 */
public final class MantoStarter {

    private MantoStarter() {}
}

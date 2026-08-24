package io.github.manto.core;

/**
 * Framework-agnostic representation of a message header.
 */
public interface MantoHeader {

    /**
     * Returns the header key.
     */
    String key();

    /**
     * Returns the header value as a string.
     */
    String value();

    /**
     * Returns the header value as bytes.
     */
    byte[] valueBytes();
}
package org.roadlessforest.xyz;

import xyz.wgs84.TileKey;

import java.io.IOException;

/**
 * Created by willtemperley@gmail.com on 13-Mar-17.
 */
public interface TileWriter {

    void append(TileKey key, int[] image) throws IOException;

    void close() throws IOException;

}

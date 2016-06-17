package org.roadlessforest.osm.raster;

/**
 * Can be implemented by raster surfaces which plot point-by-point
 *
 * Created by willtemperley@gmail.com on 29-Jun-15.
 */
public interface Plotter {

    void plot(int x, int y);

}

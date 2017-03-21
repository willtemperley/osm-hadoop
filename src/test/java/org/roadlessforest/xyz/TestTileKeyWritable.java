package org.roadlessforest.xyz;

import org.junit.Assert;
import org.junit.Test;
import xyz.wgs84.TileKey;

/**
 * Created by willtemperley@gmail.com on 10-Mar-17.
 */
public class TestTileKeyWritable {

    @Test
    public void testSerDe() {

        TileKey serKey = new TileKey();
        TileKey deKey = new TileKey();

        serKey.setDimensions(100, 200);
        serKey.setPixelSize(0.1, 0.2);
        serKey.setOrigin(11.1, 22.2);
        serKey.setProj(4326);

        TileKeyWritable tileKeyWritable = new TileKeyWritable();
        tileKeyWritable.setTile(serKey);

        tileKeyWritable.readTile(deKey);

        Assert.assertEquals(serKey.getHeight(), deKey.getHeight());
        Assert.assertEquals(serKey.getWidth(), deKey.getWidth());

        Assert.assertEquals(serKey.getOriginX(), deKey.getOriginX(), 0.00001);
        Assert.assertEquals(serKey.getOriginY(), deKey.getOriginY(), 0.00001);

        Assert.assertEquals(serKey.getPixelSizeX(), deKey.getPixelSizeX(), 0.00001);
        Assert.assertEquals(serKey.getPixelSizeY(), deKey.getPixelSizeY(), 0.00001);

        Assert.assertEquals(serKey.getProj(), deKey.getProj());

    }


}

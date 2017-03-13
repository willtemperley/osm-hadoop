package org.roadlessforest.xyz;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import xyz.wgs84.TileKey;

import java.nio.ByteBuffer;

/**
 * fixme Can we move some of the serialization into xyz?
 *
 * Created by willtemperley@gmail.com on 10-Mar-17.
 */
public class TileKeyWritable extends ImmutableBytesWritable {

    private final int bufferSize = (4 * 8) + (3 * 4);

    public void setTile(TileKey tileKey) {
        ByteBuffer buf = ByteBuffer.wrap(new byte[bufferSize]);

        buf.putDouble(tileKey.getOriginX());
        buf.putDouble(tileKey.getOriginY());
        buf.putDouble(tileKey.getPixelScaleX());
        buf.putDouble(tileKey.getPixelScaleY());
        buf.putInt(tileKey.getWidth());
        buf.putInt(tileKey.getHeight());
        buf.putInt(tileKey.getProj());

        byte[] array = buf.array();
        this.set(array);
    }

    /**
     * @param tileKey
     *
     * Object reuse encouraged
     */
    public void readTile(TileKey tileKey) {

        ByteBuffer arr = ByteBuffer.wrap(this.get());

        tileKey.setOrigin(arr.getDouble(), arr.getDouble());
        tileKey.setPixelScales(arr.getDouble(), arr.getDouble());
        tileKey.setDimensions(arr.getInt(), arr.getInt());
        tileKey.setProj(arr.getInt());
    }

}

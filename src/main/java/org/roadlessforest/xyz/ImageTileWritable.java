package org.roadlessforest.xyz;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayPrimitiveWritable;

import java.nio.ByteBuffer;

/**
 * Start with ints
 *
 * Created by willtemperley@gmail.com on 10-Mar-17.
 */
public class ImageTileWritable extends ImmutableBytesWritable {

    public void setImage(int[] ints) {

        byte[] bytes = new byte[ints.length * 4];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        for (int i : ints) {
            buffer.putInt(i);
        }

        set(buffer.array());
    }

    public int[] getImage() {

        byte[] array = this.get();
        int[] ints = new int[array.length / 4];
        ByteBuffer buffer = ByteBuffer.wrap(array);
        for (int i = 0; i < ints.length; i++) {
            ints[i] = buffer.getInt();
        }
        return ints;
    }

}

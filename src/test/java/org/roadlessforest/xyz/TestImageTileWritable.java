package org.roadlessforest.xyz;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by willtemperley@gmail.com on 10-Mar-17.
 */
public class TestImageTileWritable {

    @Test
    public void testSerDe() {

        //Arbitrary int array
        int[] ints = new int[5000000];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = i;
        }

        ImageTileWritable imageTileWritable = new ImageTileWritable();
        imageTileWritable.setImage(ints);

        int[] image = imageTileWritable.getImage();

        for (int i = 0; i < ints.length; i++) {
            Assert.assertEquals(ints[i], image[i]);
        }

    }
}

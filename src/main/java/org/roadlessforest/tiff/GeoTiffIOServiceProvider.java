package org.roadlessforest.tiff;

import com.esri.core.geometry.Envelope2D;
import org.opengis.coverage.grid.InvalidRangeException;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;
import javax.media.jai.PlanarImage;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.awt.image.renderable.ParameterBlock;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author tkunicki
 */
public class GeoTiffIOServiceProvider {

    private final static String IMAGEIO_EXT_READER =
            "it.geosolutions.imageioimpl.plugins.tiff.TIFFImageReader";
    private final static String IMAGEIO_JAI_READER =
            "com.sun.media.imageioimpl.plugins.tiff.TIFFImageReader";

    private final static String ATTRIBUTE_BAND = "band";
    private final static String ATTRIBUTE_IMAGE = "image";

    static {
        ImageIO.scanForPlugins();
    }


    public void readData(String location) throws IOException, InvalidRangeException {


        ImageReader imageReader = null;
        ImageInputStream imageInputStream = null;
        try {
            imageReader = createImageReader();

            imageInputStream = ImageIO.createImageInputStream(new File(location));
            imageReader.setInput(imageInputStream, true, true);

            ImageReadParam imageReadParam = imageReader.getDefaultReadParam();
            imageReadParam.setSourceBands(new int[]{1});
//            imageReadParam.setDestinationBands(new int[] { 0 });

            int IMG_IDX = 0;
            BufferedImage bufferedImage = imageReader.read(IMG_IDX, imageReadParam);

            // *POTENTIAL MEMORY ISSUE* Wish we could do this on read.
            // It appears that even with source/destination band specified with
            // a single entry, image comes back with storage for all bands in
            // original image (interleaved, unused bands are set to 0).
            if (imageReader.getRawImageType(IMG_IDX).getNumBands() > 1) {
                ParameterBlock parameterBlock = new ParameterBlock();
                parameterBlock.addSource(bufferedImage);
                parameterBlock.add(new int[]{0});
                PlanarImage planarImage = JAI.create("BandSelect", parameterBlock);
                bufferedImage = planarImage.getAsBufferedImage();
            }

            Raster raster = bufferedImage.getData();

//            raster.getDataElements( 0, 0, bufferedImage.getWidth(), bufferedImage.getHeight(), arrayAsObject);

//            Array array = Array.factory(
//                    variable.getDataType(),
//                    section.getShape(),
//                    arrayAsObject);

//            return array;

        } finally {
            if (imageReader != null) {
                imageReader.dispose();
            }
            if (imageInputStream != null) {
                try {
                    imageInputStream.close();
                } catch (IOException e) {
                }
            }
        }
    }

    protected ImageReader createImageReader() {
        Iterator<ImageReader> imageReaders =
                ImageIO.getImageReadersBySuffix("tiff");
        List<ImageReader> otherReaderList = new ArrayList<ImageReader>();
        ImageReader extReader = null;
        ImageReader jaiReader = null;
        while (imageReaders.hasNext()) {
            ImageReader reader = imageReaders.next();
            String readerClassName = reader.getClass().getName();
            if (IMAGEIO_EXT_READER.equals(readerClassName)) {
                extReader = reader;
            } else if (IMAGEIO_JAI_READER.equals(readerClassName)) {
                jaiReader = reader;
            } else {
                otherReaderList.add(reader);
            }
        }

        if (extReader != null) {
            return extReader;
        } else if (jaiReader != null) {
            return jaiReader;
        } else if (otherReaderList.size() > 0) {
            return otherReaderList.get(0);
        } else {
            return null;
        }
    }

}

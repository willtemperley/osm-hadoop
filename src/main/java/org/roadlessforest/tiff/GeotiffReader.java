package org.roadlessforest.tiff;

import com.esri.core.geometry.Envelope2D;
import com.sun.media.jai.codec.ByteArraySeekableStream;


import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Attempt at simplifying geotiff reading
 *
 * Created by willtemperley@gmail.com on 09-Mar-17.
 */
public class GeoTiffReader extends GeoTiffIOServiceProvider {

    List<ImageMetadata> imageMetadataList = new ArrayList<>();


    public static void main(String[] args) throws IOException {

        File file = new File("E:/osm-projects/osm-hadoop/src/test/resources/data/littletiff.tif");

        GeoTiffReader geoTiffReader = new GeoTiffReader();
        ReferencedImage referencedImage = geoTiffReader.readGeoTiff(file);

//        readGeoTiff(geoTiffIOServiceProvider, file);
    }


    private ReferencedImage readGeoTiff(File file) throws IOException {
        long length = file.length();
        byte[] bytes = new byte[(int) length];

        FileInputStream fileInputStream = new FileInputStream(file);
        int read = fileInputStream.read(bytes);
        System.out.println("read = " + read);

        return readGeotiffBytes(bytes);
    }

//    public void readMetadata(File location) {
//
//        ImageReader imageReader = null;
//        ImageInputStream imageInputStream = null;
//
//        imageReader = createImageReader();
//        try {
//            imageInputStream = ImageIO.createImageInputStream(location);
//            imageReader.setInput(imageInputStream);
//            populateMetaData(imageReader);
//
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//    }

    public ImageMetadata populateMetaData(ImageReader imageReader) throws IOException {

        int imageCount = imageReader.getNumImages(true);
//        for (int imageIdx = 0; imageIdx < imageCount; imageIdx++) {
        int imageIdx = 0;

            int imageWidthPixels = imageReader.getWidth(imageIdx);
            int imageHeightPixels = imageReader.getHeight(imageIdx);
            IIOMetadata imageMetadata = imageReader.getImageMetadata(imageIdx);

            GeoTiffIIOMetadataAdapter metadataAdapter = new GeoTiffIIOMetadataAdapter(imageMetadata);

            System.out.println("metadataAdapter = " + metadataAdapter);

            double[] pixelScales = metadataAdapter.getModelPixelScales();
            double[] tiePoints = metadataAdapter.getModelTiePoints();
            double[] transformation = metadataAdapter.getModelTransformation();

            String modelType = metadataAdapter.getGeoKey(GeoTiffIIOMetadataAdapter.GTModelTypeGeoKey);
            String geographicType = metadataAdapter.getGeoKey(GeoTiffIIOMetadataAdapter.GeographicTypeGeoKey);
            String rasterType = metadataAdapter.getGeoKey(GeoTiffIIOMetadataAdapter.GTRasterTypeGeoKey);

            String projectedCSType = metadataAdapter.getGeoKey(GeoTiffIIOMetadataAdapter.ProjectedCSTypeGeoKey);

            ImageMetadata imageMeta = new ImageMetadata(pixelScales, tiePoints, geographicType, imageWidthPixels, imageHeightPixels);//fixme

//            this.imageMetadataList.add(imageMeta);

        return imageMeta;
//        }

    }

    public ReferencedImage readGeotiffFromFile(File f) throws IOException {

        FileInputStream fileInputStream = new FileInputStream(f);
        ImageInputStream imageInputStream = ImageIO.createImageInputStream(fileInputStream);

        return getReferencedImage(imageInputStream);
    }

    public ReferencedImage readGeotiffBytes(byte[] bytes) throws IOException {

        ByteArraySeekableStream byteArraySeekableStream = new ByteArraySeekableStream(bytes);
        ImageInputStream imageInputStream = ImageIO.createImageInputStream(byteArraySeekableStream);

        return getReferencedImage(imageInputStream);
    }

    private ReferencedImage getReferencedImage(ImageInputStream imageInputStream) throws IOException {
        ImageReader imageReader = createImageReader();
        imageReader.setInput(imageInputStream);
        ImageMetadata imageMetadata = populateMetaData(imageReader);
        RenderedImage renderedImage = imageReader.readAsRenderedImage(0, null);

        ReferencedImage referencedImage = new ReferencedImage(imageMetadata, renderedImage);
        return referencedImage;
    }

    public static class ReferencedImage {
        private final ImageMetadata metadata;
        private final RenderedImage renderedImage;

        public ReferencedImage(ImageMetadata metadata, RenderedImage renderedImage) {
            this.metadata = metadata;
            this.renderedImage = renderedImage;
        }

        public ImageMetadata getMetadata() {
            return metadata;
        }

        public RenderedImage getRenderedImage() {
            return renderedImage;
        }
    }

    public static class ImageMetadata {

        final double[] pixelScales;
        final double[] tiepoints;
        final String proj;
        private final int width;
        private final int height;

        public ImageMetadata(double[] pixelScales, double[] tiepoints, String proj, int width, int height) {

            this.pixelScales = pixelScales;
            this.tiepoints = tiepoints;
            this.proj = proj;
            this.width = width;
            this.height = height;

        }
        public Envelope2D getEnvelope2D() {
            double xLeft = tiepoints[3];
            double yTop = tiepoints[4];
            double yBottom = yTop - (pixelScales[1] * height);
            double xRight = xLeft + (pixelScales[0] * width);

            Envelope2D envelope2D = new Envelope2D(xLeft, yBottom, xRight, yTop);
            return envelope2D;
        }

        public double getPixelScaleX() {
            return pixelScales[0];
        }

        public double getPixelScaleY() {
            return pixelScales[1];
        }

        public int getWidth() {
            return width;
        }

        public int getHeight() {
            return height;
        }
    }
}

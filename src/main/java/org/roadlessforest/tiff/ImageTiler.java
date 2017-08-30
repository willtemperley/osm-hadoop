package org.roadlessforest.tiff;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.resources.Arguments;
import org.opengis.geometry.Envelope;
import org.roadlessforest.xyz.ImageTileWritable;

import java.awt.image.*;
import java.io.File;
import java.io.IOException;

/**
 * Cuts up a tiff into strips rather than tiles currently and appends them to a sequence file
 *
 */
public class ImageTiler extends ImageSeqfileWriter {

    private File inputFile;

    public static void main(String[] args) throws Exception {

        //GeoTools provides utility classes to parse command line arguments
        Arguments processedArgs = new Arguments(args);
        ImageTiler tiler = new ImageTiler();

        try {
            tiler.setInputPath(new File(processedArgs.getRequiredString("-f")));
            tiler.setOutputPath(new File(processedArgs.getRequiredString("-o")));
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            printUsage();
            System.exit(1);
        }

        SequenceFile.Writer writer = getWriter(tiler.getOutputDirectory().toString());

        tiler.tile(writer);
    }

    private static void printUsage() {
        System.out.println("Usage: -f inputFile -o outputDirectory [-tw tileWidth<default:256> ");
    }


    private void writable() {
    }

    private void tile(SequenceFile.Writer writer) throws IOException {

        GeoTiffReader geoTiffReader = new GeoTiffReader();
        File inputFile = this.getInputFile();
        GeoTiffReader.ReferencedImage referencedImage = geoTiffReader.readGeotiffFromFile(inputFile);

        GeoTiffReader.ImageMetadata metadata = referencedImage.getMetadata();
        int w = metadata.getWidth();
        int h = metadata.getHeight();

        com.esri.core.geometry.Envelope2D imgEnv = metadata.getEnvelope2D();

        double geographicPixHeight = (imgEnv.ymax - imgEnv.ymin) / (double) h;
        System.out.println("geographicPixHeight = " + geographicPixHeight);

//        CoordinateReferenceSystem targetCRS = gridCoverage.getCoordinateReferenceSystem();
        RenderedImage renderedImage = referencedImage.getRenderedImage();

//        int targetTileSize = 1024;
        int targetTileSize = 1024;
        double nTiles = Math.ceil(((double) h) / targetTileSize);

        int whereAreWe = 0;

        for (int tileN = 0; tileN < nTiles; tileN++) {

            /*
            TmsTile size will be the target size unless we've reached the bottom of the image
             */
            int tileSize = targetTileSize;
            if (whereAreWe + tileSize > h) {
                tileSize = h - whereAreWe;
            }

            whereAreWe += tileSize;

            SampleModel compatibleSampleModel = renderedImage.getSampleModel().createCompatibleSampleModel(w, tileSize);
            DataBuffer dataBuffer = compatibleSampleModel.createDataBuffer();

            int tileOffsetTop = tileN * targetTileSize; //Tiles know about their offset from the top in the JAI model
            System.out.println("tileOffsetTop = " + tileOffsetTop);

            for (int i = 0; i < tileSize; i++) {

                int tileY = tileOffsetTop + i; //AKA offset from top
                System.out.println("tileY = " + tileY);

                Raster tile = null;
                try{
                   tile  = renderedImage.getTile(0, tileY);
                } catch (IllegalArgumentException e) {
                    System.out.println("e = " + e);
                }
                int[] tileArr = new int[w];
                int y = tileOffsetTop + i;
                tile.getPixels(0, y, w, 1, tileArr);
                for (int j = 0; j < tileArr.length; j++) {
                    int bufferIdx = (w * i) + j;
                    dataBuffer.setElem(bufferIdx, tileArr[j]);
                }
            }

            double geographicTileHeight = tileSize * geographicPixHeight;
            double tileTop = imgEnv.ymax - (geographicTileHeight * tileN);
            double tileBottom = tileTop - geographicTileHeight;

            Envelope envelope = new ReferencedEnvelope(imgEnv.xmin, imgEnv.xmax, tileBottom, tileTop, DefaultGeographicCRS.WGS84);

            WritableRaster writableRaster = WritableRaster.createWritableRaster(compatibleSampleModel, dataBuffer, null);
            BufferedImage bufferedImage = new BufferedImage(renderedImage.getColorModel(), writableRaster, true, null);

            GridCoverage2D tif = new GridCoverageFactory().create("tif", bufferedImage, envelope);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            new GeoTiffWriter(outputStream).write(tif, WriteParams.get(WriteParams.PackBits));

            byte[] bytes = outputStream.toByteArray();

            append(writer, tileN, bytes);
        }

        writer.close();
    }


    public File getInputFile() {
        return inputFile;
    }

    public void setInputPath(File inputFile) {
        this.inputFile = inputFile;
    }

    public File getOutputDirectory() {
        return outputDirectory;
    }

    public void setOutputPath(File outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

}
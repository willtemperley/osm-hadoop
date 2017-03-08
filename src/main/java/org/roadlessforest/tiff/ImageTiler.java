package org.roadlessforest.tiff;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.factory.Hints;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.resources.Arguments;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

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

    private void tile(SequenceFile.Writer writer) throws IOException {


        Hints hints = new Hints();
        hints.put(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
        hints.put(Hints.DEFAULT_COORDINATE_REFERENCE_SYSTEM, DefaultGeographicCRS.WGS84);
        GeoTiffReader geoTiffReader = new GeoTiffReader(this.getInputFile(), hints);
        System.out.println("hints = " + hints);
        //        GridCoverage2DReader gridReader = geoTiffReader.read(null);
        GridCoverage2D gridCoverage = geoTiffReader.read(null);
        RenderedImage renderedImage = gridCoverage.getRenderedImage();

        int w = renderedImage.getWidth();
        int h = renderedImage.getHeight();

        Envelope2D imgEnv = gridCoverage.getEnvelope2D();

        double geographicPixHeight = (imgEnv.getMaxY() - imgEnv.getMinY()) / (double) h;
        System.out.println("geographicPixHeight = " + geographicPixHeight);

        CoordinateReferenceSystem targetCRS = gridCoverage.getCoordinateReferenceSystem();

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

            int tileOffsetTop = tileN * tileSize; //Tiles know about their offset from the top

            for (int i = 0; i < tileSize; i++) {

                int tileY = (tileN * tileSize) + i; //AKA offset from top

                Raster tile = renderedImage.getTile(0, tileY);
                int[] tileArr = new int[w];
                tile.getPixels(0, tileOffsetTop + i, w, 1, tileArr);
                for (int j = 0; j < tileArr.length; j++) {
                    int bufferIdx = (w * i) + j;
                    dataBuffer.setElem(bufferIdx, tileArr[j]);
                }
            }

            double geographicTileHeight = tileSize * geographicPixHeight;
            double tileTop = imgEnv.getMaxY() - (geographicTileHeight * tileN);
            double tileBottom = tileTop - geographicTileHeight;

            Envelope envelope = new ReferencedEnvelope(imgEnv.getMinX(), imgEnv.getMaxX(), tileBottom, tileTop, targetCRS);

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
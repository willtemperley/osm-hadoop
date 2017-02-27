package org.roadlessforest.tiff;

/**
 *
 * Created by willtemperley@gmail.com on 17-Feb-17.
 */
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.factory.Hints;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.resources.Arguments;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.awt.image.*;
import java.io.File;
import java.io.IOException;

/**
 */
public class ImageTiler2 {

    private File inputFile;
    private File outputDirectory;
    Text k = new Text();
    ArrayPrimitiveWritable v = new ArrayPrimitiveWritable();


    public static void main(String[] args) throws Exception {

        //GeoTools provides utility classes to parse command line arguments
        Arguments processedArgs = new Arguments(args);
        ImageTiler2 tiler = new ImageTiler2();

        try {
            tiler.setInputFile(new File(processedArgs.getRequiredString("-f")));
            tiler.setOutputDirectory(new File(processedArgs.getRequiredString("-o")));
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            printUsage();
            System.exit(1);
        }


        Configuration conf = new Configuration();
        SequenceFile.Writer.Option fileOption = SequenceFile.Writer.file(new Path(tiler.getOutputDirectory().toString()));
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, fileOption,
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(ArrayPrimitiveWritable.class)
        );

        tiler.tile(writer);

    }

    private static void printUsage() {
        System.out.println("Usage: -f inputFile -o outputDirectory [-tw tileWidth<default:256> ");
    }

    private void tile(SequenceFile.Writer writer) throws IOException {

        AbstractGridFormat format = GridFormatFinder.findFormat(this.getInputFile());
        String fileExtension = this.getFileExtension(this.getInputFile());

        //working around a bug/quirk in geotiff loading via format.getReader which doesn't set this
        //correctly
        Hints hints = null;
        if (format instanceof GeoTiffFormat) {
            hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
        }

        GridCoverage2DReader gridReader = format.getReader(this.getInputFile(), hints);
        GridCoverage2D gridCoverage = gridReader.read(null);

        RenderedImage renderedImage = gridCoverage.getRenderedImage();

        int w = renderedImage.getWidth();
        int h = renderedImage.getHeight();

        Envelope2D imgEnv = gridCoverage.getEnvelope2D();

        double geographicPixHeight = (imgEnv.getMaxY() - imgEnv.getMinY()) / (double) h;
        System.out.println("geographicPixHeight = " + geographicPixHeight);

        CoordinateReferenceSystem targetCRS = gridCoverage.getCoordinateReferenceSystem();

        //make sure to create our output directory if it doesn't already exist
//        File tileDirectory = this.getOutputDirectory();
//        if (!tileDirectory.exists()) {
//            tileDirectory.mkdirs();
//        }

        int targetTileSize = 1024;
        double nTiles = Math.ceil(((double) h) / targetTileSize);

        int whereAreWe = 0;

        for (int tileN = 0; tileN < nTiles; tileN++) {

            int tileSize = targetTileSize;

            if (whereAreWe + tileSize > h) {
                tileSize = h - whereAreWe;
            }

            whereAreWe += tileSize;

            int dataBufferSize = tileSize * w;

            SampleModel compatibleSampleModel = renderedImage.getSampleModel().createCompatibleSampleModel(w, tileSize);
            byte[] arr = new byte[dataBufferSize];


            DataBufferByte dataBuffer = new DataBufferByte(arr, dataBufferSize);

            int tileOffsetTop = tileN * tileSize; //Tiles know about their offset from the top

            for (int i = 0; i < tileSize; i++) {

                int tileY = (tileN * tileSize) + i; //AKA offset from top

                Raster tile = renderedImage.getTile(0, tileY);
                int[] tileArr = new int[w];
                tile.getPixels(0, tileOffsetTop + i, w, 1, tileArr);
                for (int j = 0; j < tileArr.length; j++) {
                    arr[(w * i) + j] = (byte) tileArr[j];
                }
            }

            double geographicTileHeight = tileSize * geographicPixHeight;

            double tileTop = imgEnv.getMaxY() - (geographicTileHeight * tileN);
            double tileBottom = tileTop - geographicTileHeight;

            Envelope envelope = new ReferencedEnvelope(imgEnv.getMinX(), imgEnv.getMaxX(), tileBottom, tileTop, targetCRS);

            WritableRaster writableRaster = WritableRaster.createWritableRaster(compatibleSampleModel, dataBuffer, null);
            BufferedImage bufferedImage = new BufferedImage(renderedImage.getColorModel(), writableRaster, true, null);

            GridCoverage2D tif = new GridCoverageFactory().create("tif", bufferedImage, envelope);
            String fileName = "tileN_" + tileN + "." + fileExtension;
            System.out.println("fileName = " + fileName);
//            File tileFile = new File(tileDirectory, fileName);


            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            format.getWriter(outputStream).write(tif, WriteParams.get(WriteParams.PackBits));

            byte[] bytes = outputStream.toByteArray();


            k.set(""+tileN);
            v.set(bytes);
            writer.append(k, v);
        }

        writer.close();
    }

    private String getFileExtension(File file) {
        String name = file.getName();
        try {
            return name.substring(name.lastIndexOf(".") + 1);
        } catch (Exception e) {
            return "";
        }
    }

    public File getInputFile() {
        return inputFile;
    }

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    public File getOutputDirectory() {
        return outputDirectory;
    }

    public void setOutputDirectory(File outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

}
package org.roadlessforest.xyz;


import org.roadlessforest.tiff.GeoTiffReader;

import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;

import org.geotools.resources.Arguments;
import xyz.wgs84.TileKey;

/**
 * Created by willtemperley@gmail.com on 10-Mar-17.
 */
public class ImageTiler {

//    protected ImageTiler(String outputDirectory) throws IOException {
//        super(outputDirectory);
//    }

    public static void main(String[] args) throws Exception {

        //GeoTools provides utility classes to parse command line arguments
        Arguments processedArgs = new Arguments(args);

        ImageTiler tiler = new ImageTiler();

        String inPath = processedArgs.getRequiredString("-f");
        String outPath = processedArgs.getRequiredString("-o");

        ImageSeqFileWriter writer = new ImageSeqFileWriter(outPath);
        tiler.doTiling(inPath, writer);
    }

    private void doTiling(String outputFilePath, ImageSeqFileWriter writer) throws IOException {

        GeoTiffReader geoTiffReader = new GeoTiffReader();
        GeoTiffReader.ReferencedImage referencedImage = geoTiffReader.readGeotiffFromFile(new File(outputFilePath));

        GeoTiffReader.ImageMetadata metadata = referencedImage.getMetadata();
        int w = metadata.getWidth();
        int h = metadata.getHeight();

        com.esri.core.geometry.Envelope2D imgEnv = metadata.getEnvelope2D();

        double geographicPixHeight = (imgEnv.ymax - imgEnv.ymin) / (double) h;

        RenderedImage renderedImage = referencedImage.getRenderedImage();

        int targetTileSize = 1024;
        double nTiles = Math.ceil(((double) h) / targetTileSize);

        int whereAreWe = 0;

        ImageTileWritable imageTileWritable = new ImageTileWritable();

        //fixme hack for non GEE images
        if (nTiles == 1) {

            int[] IMAGE = new int[w * h];
            Raster data = renderedImage.getData();
            data.getPixels(0,0, w, h, IMAGE);

            TileKey tileKey = new TileKey();
            tileKey.setOrigin(imgEnv.xmin, imgEnv.ymax);
            tileKey.setDimensions(w, h);
            tileKey.setProj(4326);
            tileKey.setPixelSize(geographicPixHeight, geographicPixHeight); //fixme from tiff meta??

            writer.append(tileKey, IMAGE);

            writer.close();
            return;
        }

        for (int tileN = 0; tileN < nTiles; tileN++) {

            /*
            TmsTile size will be the target size unless we've reached the bottom of the image
             */
            int tileHeight = targetTileSize;
            if (whereAreWe + tileHeight > h) {
                tileHeight = h - whereAreWe;
            }
            whereAreWe += tileHeight;


            int[] IMAGE = new int[w * tileHeight];

//            SampleModel compatibleSampleModel = renderedImage.getSampleModel().createCompatibleSampleModel(w, tileHeight);
//            DataBuffer dataBuffer = compatibleSampleModel.createDataBuffer();

            int tileOffsetTop = tileN * targetTileSize; //Tiles know about their offset from the top in the JAI model
            System.out.println("tileOffsetTop = " + tileOffsetTop);

            for (int i = 0; i < tileHeight; i++) {

                int tileY = tileOffsetTop + i; //AKA offset from top
                System.out.println("tileY = " + tileY);

                Raster tile = renderedImage.getTile(0, tileY);
                int[] tileArr = new int[w];
                int y = tileOffsetTop + i;
                tile.getPixels(0, y, w, 1, tileArr);

                int bufferOffset = i * w; //row * wi
                for (int j = 0; j < tileArr.length; j++) {
                    int k = bufferOffset + j;
                    IMAGE[k] = tileArr[j];
                }

            }

            imageTileWritable.setImage(IMAGE);

            double geographicTileHeight = tileHeight * geographicPixHeight;
            double tileTop = imgEnv.ymax - (geographicTileHeight * tileN);
            double tileBottom = tileTop - geographicTileHeight;

//            Envelope envelope = new ReferencedEnvelope(imgEnv.xmin, imgEnv.xmax, tileBottom, tileTop, DefaultGeographicCRS.WGS84);
            TileKey tileKey = new TileKey();
            tileKey.setOrigin(imgEnv.xmin, imgEnv.ymax);
            tileKey.setDimensions(w, tileHeight);
            tileKey.setProj(4326);
            tileKey.setPixelSize(geographicPixHeight, geographicPixHeight); //fixme from tiff meta??

            writer.append(tileKey, IMAGE);

//            append(writer, tileN, bytes);
        }

        writer.close();
    }

}

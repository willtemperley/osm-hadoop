package org.roadlessforest.osm;

import org.apache.hadoop.io.SequenceFile;
import org.geotools.resources.Arguments;
import org.roadlessforest.tiff.ImageSeqfileWriter;

import java.io.File;
import java.io.FileInputStream;

/**
 * Very dumb class, used simply to build a single-record sequence file
 */
public class ImageTilerTest extends ImageSeqfileWriter {

    private File inputFile;

    public static void main(String[] args) throws Exception {

        //GeoTools provides utility classes to parse command line arguments
        Arguments processedArgs = new Arguments(args);
        ImageTilerTest tiler = new ImageTilerTest();

        try {
            tiler.setInputFile(new File(processedArgs.getRequiredString("-f")));
            tiler.setOutputDirectory(new File(processedArgs.getRequiredString("-o")));
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }

        SequenceFile.Writer writer = getWriter(tiler.getOutputDirectory().toString());

        File inputFile = tiler.getInputFile();

        FileInputStream fileInputStream = new FileInputStream(inputFile);
        byte[] bytes = new byte[(int) inputFile.length()];
        fileInputStream.read(bytes);

        tiler.append(writer, 100, bytes);

        writer.close();
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
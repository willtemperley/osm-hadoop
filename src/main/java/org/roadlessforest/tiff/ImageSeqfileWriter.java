package org.roadlessforest.tiff;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

/**
 * Created by willtemperley@gmail.com on 01-Mar-17.
 */
public class ImageSeqfileWriter {
    protected Text k = new Text();
    protected ArrayPrimitiveWritable v = new ArrayPrimitiveWritable();
    protected File outputDirectory;

    protected static SequenceFile.Writer getWriter(String outputDirectory) throws IOException {
        Configuration conf = new Configuration();
        SequenceFile.Writer.Option fileOption = SequenceFile.Writer.file(new Path(outputDirectory));
        return SequenceFile.createWriter(conf, fileOption,
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(ArrayPrimitiveWritable.class)
        );
    }

    protected void append(SequenceFile.Writer writer, int tileN, byte[] bytes) throws IOException {
        k.set(""+tileN); //it doesn't matter
        v.set(bytes);
        writer.append(k, v);
    }
}

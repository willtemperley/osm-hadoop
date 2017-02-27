package org.roadlessforest.tiff;

import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.factory.Hints;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.gce.geotiff.GeoTiffWriteParams;
import org.geotools.parameter.DefaultParameterDescriptor;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.parameter.ParameterValueGroup;

import javax.imageio.ImageWriteParam;
import java.util.List;

/**
 * Created by willtemperley@gmail.com on 21-Feb-17.
 */
public class WriteParams {

    public static String CCITT_RLE = "CCITT RLE";
    public static String CCITT_T_4 = "CCITT T.4";
    public static String CCITT_T_6 = "CCITT T.6";
    public static String LZW = "LZW";
    public static String JPEG = "JPEG";
    public static String Zlib = "ZLib";
    public static String PackBits = "PackBits";
    public static String Deflate = "Deflate";
    public static String EXIF_JPEG = "EXIF JPEG";

    /**
     * Just to try and keep these horrors in one place
     *
     * @return
     */
    public static GeneralParameterValue[] get(String compressionType) {

        GeoTiffWriteParams wp = new GeoTiffWriteParams();

        wp.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        wp.setCompressionType(compressionType);
        wp.setCompressionQuality(1.0F);

        GeoTiffFormat format = new GeoTiffFormat();

        ParameterValueGroup params = format.getWriteParameters();

        params.parameter(AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.getName().toString()).setValue(wp);

        List<GeneralParameterValue> x = params.values();

        DefaultParameterDescriptor<Boolean> retainAxes = GeoTiffFormat.RETAIN_AXES_ORDER;
        ParameterValue<Boolean> retainAxesValue = retainAxes.createValue();
        retainAxesValue.setValue(true);

        x.add(retainAxesValue);
        return x.toArray(new GeneralParameterValue[2]);
    }

    public static Hints longitudeFirst = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);

}

package org.roadlessforest.xyz;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

import static org.osgeo.proj4j.parser.Proj4Keyword.f;

/**
 * Created by willtemperley@gmail.com on 13-Mar-17.
 */
public class ConfigurationFactory {

    public static Properties getProperties(String propertiesFileName) throws IOException {
        Properties props = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream(propertiesFileName);
        props.load(resourceStream);
        return props;
    }

    public static Properties hbaseProperties() throws IOException {
        return getProperties("hbase-config.properties");
    }


    public static Configuration getConfiguration() throws IOException {

        Configuration configuration = new Configuration();
        Properties properties = hbaseProperties();
        Enumeration<?> enumeration = properties.propertyNames();

        while (enumeration.hasMoreElements()) {
            Object key = enumeration.nextElement();
            Object o = properties.get(key);
            configuration.set(key.toString(), o.toString());
        }

        return configuration;
    }

    public static void main(String[] args) throws IOException {
        Configuration configuration = getConfiguration();
        System.out.println("configuration = " + configuration);
    }
}

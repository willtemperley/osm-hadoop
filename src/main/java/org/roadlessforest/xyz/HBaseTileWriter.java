package org.roadlessforest.xyz;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import xyz.wgs84.TileKey;

import java.io.IOException;

/**
 * Created by willtemperley@gmail.com on 13-Mar-17.
 */
public class HBaseTileWriter implements TileWriter {


    public HBaseTileWriter() {

        try {
            Connection connection = ConnectionFactory.createConnection(ConfigurationFactory.getConfiguration());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void append(TileKey key, int[] image) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}

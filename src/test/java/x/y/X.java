package x.y;

import com.google.inject.Guice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import org.openstreetmap.osmosis.hbase.CellReducer;
import org.openstreetmap.osmosis.hbase.MapReduceUnitSetup;
import org.openstreetmap.osmosis.hbase.MockHTableModule;
import org.openstreetmap.osmosis.hbase.TableModule;
import org.openstreetmap.osmosis.hbase.common.Entity;
import org.openstreetmap.osmosis.hbase.common.TableFactory;
import org.openstreetmap.osmosis.hbase.mr.OsmEntityMapper;
import org.openstreetmap.osmosis.hbase.mr.WayMapper;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfRawBlob;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.PbfStreamSplitter;

import java.io.*;
import java.util.List;

/**
 * Created by willtemperley@gmail.com on 03-Nov-16.
 */
public class X extends MapReduceUnitSetup {

    public X() {
        injector = Guice.createInjector(new MockHTableModule());
    }

    @Test
    public void test() throws IOException {
        File snapshotBinaryFile = new File("src/test/resources/data/template/v0_6/db-snapshot.pbf");

        loadTable(snapshotBinaryFile, "ways", new WayMapper());
    }

    protected <T extends Entity> void loadTable(File snapshotBinaryFile, String tableName, OsmEntityMapper<T> mapper) throws IOException {

        TableFactory hTableFact = getTableFactory();

        MapReduceDriver<Text, ArrayPrimitiveWritable, ImmutableBytesWritable, Cell, ImmutableBytesWritable, Put> mapReduceDriver = MapReduceDriver.newMapReduceDriver();
        Table mockTable = hTableFact.getTable(tableName);

        CellReducer cellReducer = new CellReducer();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(cellReducer);

        //Set up config with some settings that would normally be set in HFileOutputFormat2.configureIncrementalLoad();
        setupSerialization(mapReduceDriver);



        InputStream inputStream = new FileInputStream(snapshotBinaryFile);
        PbfStreamSplitter streamSplitter = new PbfStreamSplitter(new DataInputStream(inputStream));

        ArrayPrimitiveWritable arrayPrimitiveWritable = new ArrayPrimitiveWritable();
        Text text = new Text();

        while (streamSplitter.hasNext()) {
            PbfRawBlob blob = streamSplitter.next();
            arrayPrimitiveWritable.set(blob.getData());
            String type = blob.getType();
            text.set(type);
            mapReduceDriver.withInput(text, arrayPrimitiveWritable);
        }

        //Retrieve MR results
        List<Pair<ImmutableBytesWritable, Put>> results = mapReduceDriver.run();
        for (Pair<ImmutableBytesWritable, Put> cellPair : results) {
            mockTable.put(cellPair.getSecond());
        }
    }

}

OSM-Hadoop
==========
This library has been created to generate global raster maps of OpenStreetMap (OSM) data as an input to a global accessiblity mapping project.
It is designed to take a heavily compressed OSM planet dump file, in protocol buffer format (pbf) and extract geographical entities and raster maps.
The OSM planet dump file can be found on various mirrors. See: http://wiki.openstreetmap.org/wiki/Planet.osm

Why Hadoop?
===========
The OSM data model consists of nodes, ways and relations.
Geographic information (x,y coordinates) is only stored within nodes, and their relationship to one another is described through a list of node ids held within ways, known as way-nodes.
Therefore to construct a way, the node corresponding to each way-node must be found. Two ways forming part of a road network may share a common node - i.e. transport infrastructure data are stored as a topological network.
The attributes of both nodes and ways are held as a set of key-value pairs.

The upshot using of such a normalised data model is that to obtain geographic entities such as linestrings and polygons, 
the nodes and ways must be joined in the order specified. At the global scale this becomes be a computationally intensive task because either a complete list of ~3 billion node locations must be held, indexed by node-id, or as is the case here all the nodes must be "mapped" and joined to the way-nodes - see step 1.  
Performing this processing on a single machine can take weeks, as is the case of populating a spatial database with the OSM data using the Osmosis toolchain.  On a nine-node cluster with 72 cores the data parsing, joining, building of spatial entities and the rasterization can be completed in less than half an hour.

It may be worth noting that relations store the relationship between ways, for example describing the constituent parts of a multi-polygon, however these are not relevant to the tasks performed here.


Building
========
Apache Maven is used to build the project. The runtime environment requirements are Hadoop (map-reduce v2) and Spark version > 1.2. All other dependencies will be resolved by Maven at build time.
There is a strong dependence on Osmosis (http://wiki.openstreetmap.org/wiki/Osmosis) which performs the parsing of the protocol buffer encoded OSM entities.

The code is easiest to run on a Hadoop cluster as an "uber-jar".  This can be built with Maven:

```
mvn clean package 
```

or

```
mvn clean package -DskipTests
```

Maven Shade is employed to preprocess the dependencies, significantly reducing the overall jar size and relocating some classes in order to prevent version clashes on the server classpath. 
This has been a problem with both Guava and Google Protocol Buffers: without relocating the classes the server version polluted the application classpath with an older incompatible version.


Preprocessing
=============

Unfortunately, the OSM protobuf format does not have delimiters between file blocks. 
When a large file is split for processing on a cluster, split boundaries will fall within data blocks. 
When this happens, the input format must skip to the beginning of a new data block, which is not possible without a clear delimiter. 
This makes it almost impossible to process in parallel on a cluster.
(When a data block falls off the end of a split, the input format must also read to the end of the data block, this is possible without a delimiter.)

The workaround is to pre-process the protobuf file sequentially, splitting the blocks into a sequencefile, which can then be read with a standard sequence file input format.

```
mvn dependency:copy-dependencies #get the classpath jars in one place
```

```
java -cp target/osm-hadoop-0.1.jar:target/dependency/* org.roadlessforest.osm.PreprocessPbf /path/to/pbf /path/to/sequencefile
```

The output path can either be an HDFS pathname or a local file path. If the latter is chosen, the file must be loaded onto the cluster as an additional step.


Hadoop data
===========
All OSM types are written as OsmEntityWritables, which is a generic writable.


Step 1 - Join Nodes and Way Nodes
=================================
All nodes and ways are read from the sequence file containing Protobuf file blocks.
Nodes (id, lat, lon) and Waynodes (node_id, way_id, ordinal) are joined on their node id (by mapping to node ids on the map-side).
Reduce-side they are written out against their respective way ids. 
Ways are treated separately and are mapped to their own ids.
This does mean that node and way ids are mixed on the reduce side, however they are 

Result: a sequence file with way ids against the georeferenced way nodes and ways.

Usage: 
```
NodeJoiner <input seq> <output seq> <tag>
```

e.g.:
```
hadoop jar target/osm-hadoop-0.1.jar org.roadlessforest.osm.NodeJoiner  /your/home/pbf/planet-pbf.seq /your/home/osm/hwynodes highway
```


Step 2 - Build Ways
===================
The ways and referenced way-nodes resulting from step 1 are grouped by way-id. In step 1 therefore ways and nodes are mapped, and ways and way-nodes are joined to create the actual ways.
For simplicity, the  is stored within the WayWritable as well-known-text, using the key "geometry".

```
hadoop jar target/osm-hadoop-0.1.jar org.roadlessforest.osm.WayBuilder  /your/home/osm/hwynodes /your/home/osm/highways
```

Step 3 - Rasterize
============================
The rasterization work is performed map-side, with pixels being mapped using their coordinates as a key (strung together bit-wise into a synthetic long value).  
Reduce-side, the pixel value of interest is filtered out, according to the preferred priority. 
These are written as (x,y),z key-value pairs where x and y are the raster indices and z is the raster value.

```
hadoop jar target/osm-hadoop-0.1.jar org.roadlessforest.osm.WayRasterizer /your/home/osm/highways /your/home/osm/xyz
```


Step 4 - Extract raster
============================
This simple Spark job burns the (x,y),z key value pairs into the raster.
The driver will be producing a large GeoTiff and may therefore require a large quantity of memory.

```
spark-submit --class org.roadlessforest.osm.ExtractRaster --master yarn-client --driver-memory=4G target/osm-hadoop-0.1.jar /your/home/osm/xyz/ /tmp/planet.tif
```



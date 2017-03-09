BareTif
=======

All I wanted to do was read a byte[] into a GeoTiff.  All I got was opaque errors.

I took apart all the libraries I know that read GeoTIFFs and found one class written in 2004 that seems to have all the magic I needed.

So, this library is the simplest way I could find of getting at the raw data in a Geotiff.
With a strongly typed interface, because Java is not duck-typed.

It was written to avoid all the SPIs that load other SPIs that load loaders that take bare objects that fail.

 

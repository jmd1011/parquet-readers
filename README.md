Fauxquet (pronounced "Foe-kay", or "Foe-kit" if in a bad mood)

This repo is now dedicated to Fauxquet (though the Fauxquet project may soon be made available elsewhere...more to come on that later).

Fauxquet is an Apache Parquet reader/writer which does not use Apache Spark. Furthermore, Fauxquet loads entire Parquet files into memory rather than using a seekable stream. This is done as part of an ongoing research effort called Flare (https://flaredata.github.io/), in which "scaled up" machines outperform "scaled out" systems.

Fauxquet is still under active development, and therefore both prone to bugs and incomplete.

Still on the way:
- Reading data from Parquet files (only metadata is currently being read in)
- Writing to Parquet files

It is expected that this project will be completed by the end of February 2017.

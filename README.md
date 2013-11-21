# parquet-cascalog

Taps for dumping cascalog queries into parquet form.  Can only be
used as a sink at the moment.

## Usage

```
(use 'cascalog.api)
(require '[parquet-cascalog.tap :as parquet])

(?<- (hfs-parquet 
      [["?foo" "int"] ["?bar" "double"] ["?baz" "string"]]
      "/tmp/foo-path")
     [?foo ?bar ?baz ?quux]
     (my awsome query ...))
```

It is important to note that the field names supplied in the schema
supplied to the tap thing actually have to correspond with the names
of your output variables.

Any output variables not used in the schema can still be used for other
stuff, for example, as inputs to :templatefields for partitioning your
output into subdirectories.

## License

Copyright © 2013 pulse.io

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

---
layout: docs
title: Future Work
permalink: /docs/future-work/
---

This is very much still a work-in-progress as there's a learning curve for an Accumulo developer to integrate with Pig.
Some things do immediately jump out as logical, low-hanging fruit.

1. **Accumulo version support** -- The initial prototype work was done against the Accumulo 1.4 release line. Accumulo 1.5
   is also a stable release. Supporting 1.5 is mostly a merge exercise. Accumulo 1.6 is also around the corner so
   targeting 1.6 compatibility is also desired.
2. **LoadPushDown** -- _AccumuloStorage_ is pretty much already doing this; however, doing it the "Pig way" is a good idea.
   Additionally, find ways to push down more logic to Accumulo instead of bringing the data into mapreduce. Regex
   filtering is a good example of things that Accumulo already has support for.
3. **Metadata** -- Despite Accumulo not requiring metadata information about the records being stored, having this
   information, in addition to statistics on records and columns, is invaluable when making cost-based optimization
   decisions. Providing a general interface for variations on _AccumuloStorage_ seems relevant.
4. **Visibility filtering** -- Working with column visibilities is currently very pedantic. Find a way that we can better
   leverage the column visibility (cell-level visibility) support for either visibility filtering or as another
   dimension of filtering over datasets stored within the same table.
5. **Support for common table structures** -- Some Accumulo table structures arise as common practices (e.g. an inverted
   index, document partitioned index, edge list). Where it makes sense, it would be nice to codify some of these formats
   into custom Storage implementations for their respective formats.
6. **Additional type support** -- Supports for storing _Bag_s currently doesn't exist. There are also likely other glaring
   holes in handling a variety of types.
7. **API cleanup** -- Specifying a CSV of column names when writing data to Accumulo with _AccumuloStorage_ is very
   pedantic and verbose. There is likely a better way to manage this.

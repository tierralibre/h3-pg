<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [API Reference](#api-reference)
- [Base type](#base-type)
    - [`h3index` :: `bigint`](#h3index--bigint)
    - [`bigint` :: `h3index`](#bigint--h3index)
- [Indexing functions](#indexing-functions)
    - [h3_geo_to_h3(`point`, resolution `integer`) ⇒ `h3index`](#h3_geo_to_h3point-resolution-integer-%E2%87%92-h3index)
    - [h3_to_geo(`h3index`) ⇒ `point`](#h3_to_geoh3index-%E2%87%92-point)
    - [h3_to_geo_boundary(`h3index`, [extend_at_meridian `BOOLEAN` = `false`]) ⇒ `polygon`](#h3_to_geo_boundaryh3index-extend_at_meridian-boolean--false-%E2%87%92-polygon)
- [Index inspection functions](#index-inspection-functions)
    - [h3_get_resolution(`h3index`) ⇒ `integer`](#h3_get_resolutionh3index-%E2%87%92-integer)
    - [h3_get_base_cell(`h3index`) ⇒ `integer`](#h3_get_base_cellh3index-%E2%87%92-integer)
    - [h3_is_valid(`h3index`) ⇒ `bool`](#h3_is_validh3index-%E2%87%92-bool)
    - [h3_is_res_class_iii(`h3index`) ⇒ `bool`](#h3_is_res_class_iiih3index-%E2%87%92-bool)
    - [h3_is_pentagon(`h3index`) ⇒ `bool`](#h3_is_pentagonh3index-%E2%87%92-bool)
    - [h3_get_faces(`h3index`) ⇒ `integer`](#h3_get_facesh3index-%E2%87%92-integer)
- [Grid traversal functions](#grid-traversal-functions)
    - [h3_k_ring(`h3index`, [k `integer` = 1]) ⇒ `h3index`](#h3_k_ringh3index-k-integer--1-%E2%87%92-h3index)
    - [h3_k_ring_distances(`h3index`, [k `integer` = 1], index `h3index`, distance `int`) ⇒ `record`](#h3_k_ring_distancesh3index-k-integer--1-index-h3index-distance-int-%E2%87%92-record)
    - [h3_hex_ring(`h3index`, [k `integer` = 1]) ⇒ `h3index`](#h3_hex_ringh3index-k-integer--1-%E2%87%92-h3index)
    - [h3_line(`h3index`, `h3index`) ⇒ `h3index`](#h3_lineh3index-h3index-%E2%87%92-h3index)
    - [h3_distance(`h3index`, `h3index`) ⇒ `integer`](#h3_distanceh3index-h3index-%E2%87%92-integer)
    - [h3_experimental_h3_to_local_ij(origin `h3index`, index `h3index`) ⇒ `POINT`](#h3_experimental_h3_to_local_ijorigin-h3index-index-h3index-%E2%87%92-point)
    - [h3_experimental_local_ij_to_h3(origin `h3index`, coord `POINT`) ⇒ `h3index`](#h3_experimental_local_ij_to_h3origin-h3index-coord-point-%E2%87%92-h3index)
- [Hierarchical grid functions](#hierarchical-grid-functions)
    - [h3_to_parent(`h3index`, [resolution `integer` = -1]) ⇒ `h3index`](#h3_to_parenth3index-resolution-integer---1-%E2%87%92-h3index)
    - [h3_to_children(`h3index`, [resolution `integer` = -1]) ⇒ `h3index`](#h3_to_childrenh3index-resolution-integer---1-%E2%87%92-h3index)
    - [h3_to_center_child(`h3index`, [resolution `integer` = -1]) ⇒ `h3index`](#h3_to_center_childh3index-resolution-integer---1-%E2%87%92-h3index)
    - [h3_compact(`h3index`) ⇒ `h3index`](#h3_compacth3index-%E2%87%92-h3index)
    - [h3_uncompact(`h3index`, [resolution `integer` = -1]) ⇒ `h3index`](#h3_uncompacth3index-resolution-integer---1-%E2%87%92-h3index)
    - [h3_to_children_slow(index `h3index`, [resolution `integer` = -1]) ⇒ `h3index`](#h3_to_children_slowindex-h3index-resolution-integer---1-%E2%87%92-h3index)
- [Region functions](#region-functions)
    - [h3_polyfill(exterior `polygon`, holes `polygon`, [resolution `integer` = 1]) ⇒ `h3index`](#h3_polyfillexterior-polygon-holes-polygon-resolution-integer--1-%E2%87%92-h3index)
    - [h3_set_to_multi_polygon(`h3index`, exterior `polygon`, holes `polygon`) ⇒ `record`](#h3_set_to_multi_polygonh3index-exterior-polygon-holes-polygon-%E2%87%92-record)
- [Unidirectional edge functions](#unidirectional-edge-functions)
    - [h3_indexes_are_neighbors(`h3index`, `h3index`) ⇒ `boolean`](#h3_indexes_are_neighborsh3index-h3index-%E2%87%92-boolean)
    - [h3_get_h3_unidirectional_edge(origin `h3index`, destination `h3index`) ⇒ `h3index`](#h3_get_h3_unidirectional_edgeorigin-h3index-destination-h3index-%E2%87%92-h3index)
    - [h3_unidirectional_edge_is_valid(edge `h3index`) ⇒ `boolean`](#h3_unidirectional_edge_is_validedge-h3index-%E2%87%92-boolean)
    - [h3_get_origin_h3_index_from_unidirectional_edge(edge `h3index`) ⇒ `h3index`](#h3_get_origin_h3_index_from_unidirectional_edgeedge-h3index-%E2%87%92-h3index)
    - [h3_get_destination_h3_index_from_unidirectional_edge(edge `h3index`) ⇒ `h3index`](#h3_get_destination_h3_index_from_unidirectional_edgeedge-h3index-%E2%87%92-h3index)
    - [h3_get_h3_indexes_from_unidirectional_edge(edge `h3index`, origin `h3index`, destination `h3index`) ⇒ `record`](#h3_get_h3_indexes_from_unidirectional_edgeedge-h3index-origin-h3index-destination-h3index-%E2%87%92-record)
    - [h3_get_h3_unidirectional_edges_from_hexagon(`h3index`) ⇒ `h3index`](#h3_get_h3_unidirectional_edges_from_hexagonh3index-%E2%87%92-h3index)
    - [h3_get_h3_unidirectional_edge_boundary(edge `h3index`) ⇒ `polygon`](#h3_get_h3_unidirectional_edge_boundaryedge-h3index-%E2%87%92-polygon)
- [Miscellaneous H3 functions](#miscellaneous-h3-functions)
    - [h3_point_dist(a `point`, b `point`, [unit `text` = km]) ⇒ `float`](#h3_point_dista-point-b-point-unit-text--km-%E2%87%92-float)
    - [h3_hex_area(resolution `integer`, [unit `text` = km]) ⇒ `float`](#h3_hex_arearesolution-integer-unit-text--km-%E2%87%92-float)
    - [h3_cell_area(cell `h3index`, [unit `text` = km^2]) ⇒ `float`](#h3_cell_areacell-h3index-unit-text--km%5E2-%E2%87%92-float)
    - [h3_edge_length(resolution `integer`, [unit `text` = km]) ⇒ `float`](#h3_edge_lengthresolution-integer-unit-text--km-%E2%87%92-float)
    - [h3_exact_edge_length(edge `h3index`, [unit `text` = km]) ⇒ `float`](#h3_exact_edge_lengthedge-h3index-unit-text--km-%E2%87%92-float)
    - [h3_num_hexagons(resolution `integer`) ⇒ `bigint`](#h3_num_hexagonsresolution-integer-%E2%87%92-bigint)
    - [h3_get_res_0_indexes() ⇒ `h3index`](#h3_get_res_0_indexes-%E2%87%92-h3index)
    - [h3_get_pentagon_indexes(resolution `integer`) ⇒ `h3index`](#h3_get_pentagon_indexesresolution-integer-%E2%87%92-h3index)
- [Operators](#operators)
  - [B-tree operators](#b-tree-operators)
    - [Operator: `h3index` = `h3index`](#operator-h3index--h3index)
    - [Operator: `h3index` <> `h3index`](#operator-h3index--h3index)
  - [R-tree Operators](#r-tree-operators)
    - [Operator: `h3index` && `h3index`](#operator-h3index--h3index)
    - [Operator: `h3index` @> `h3index`](#operator-h3index--h3index)
    - [Operator: `h3index` <@ `h3index`](#operator-h3index--h3index)
    - [Operator: `h3index` <-> `h3index`](#operator-h3index---h3index)
- [Extension specific functions](#extension-specific-functions)
    - [h3_get_extension_version() ⇒ `text`](#h3_get_extension_version-%E2%87%92-text)
- [PostGIS Functions](#postgis-functions)
    - [h3_geo_to_h3(`geometry`, resolution `integer`) ⇒ `h3index`](#h3_geo_to_h3geometry-resolution-integer-%E2%87%92-h3index)
    - [h3_geo_to_h3(`geography`, resolution `integer`) ⇒ `h3index`](#h3_geo_to_h3geography-resolution-integer-%E2%87%92-h3index)
    - [h3_to_geometry(`h3index`) ⇒ `geometry`](#h3_to_geometryh3index-%E2%87%92-geometry)
    - [h3_to_geography(`h3index`) ⇒ `geography`](#h3_to_geographyh3index-%E2%87%92-geography)
    - [h3_to_geo_boundary_geometry(`h3index`, [extend `BOOLEAN` = `false`]) ⇒ `geometry`](#h3_to_geo_boundary_geometryh3index-extend-boolean--false-%E2%87%92-geometry)
    - [h3_to_geo_boundary_geography(`h3index`, [extend `BOOLEAN` = `false`]) ⇒ `geography`](#h3_to_geo_boundary_geographyh3index-extend-boolean--false-%E2%87%92-geography)
    - [h3_polyfill(multi `geometry`, resolution `integer`) ⇒ `h3index`](#h3_polyfillmulti-geometry-resolution-integer-%E2%87%92-h3index)
    - [h3_polyfill(multi `geography`, resolution `integer`) ⇒ `h3index`](#h3_polyfillmulti-geography-resolution-integer-%E2%87%92-h3index)
  - [PostGIS casts](#postgis-casts)
    - [`h3index` :: `point`](#h3index--point)
    - [`h3index` :: `geometry`](#h3index--geometry)
    - [`h3index` :: `geography`](#h3index--geography)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# API Reference

# Base type









### `h3index` :: `bigint`


Convert H3 index to bigint, which is useful when you need a decimal representation



### `bigint` :: `h3index`


Convert bigint to H3 index

# Indexing functions

These function are used for finding the H3 index containing coordinates,
and for finding the center and boundary of H3 indexes.

### h3_geo_to_h3(`point`, resolution `integer`) ⇒ `h3index`
*Since v0.2.0*


Indexes the location at the specified resolution



### h3_to_geo(`h3index`) ⇒ `point`
*Since v1.0.0*


Finds the centroid of the index



### h3_to_geo_boundary(`h3index`, [extend_at_meridian `BOOLEAN` = `false`]) ⇒ `polygon`
*Since v1.0.0*


Finds the boundary of the index, second argument extends coordinates when crossing 180th meridian to help visualization

# Index inspection functions

These functions provide metadata about an H3 index, such as its resolution
or base cell, and provide utilities for converting into and out of the
64-bit representation of an H3 index.

### h3_get_resolution(`h3index`) ⇒ `integer`
*Since v1.0.0*


Returns the resolution of the index



### h3_get_base_cell(`h3index`) ⇒ `integer`
*Since v1.0.0*


Returns the base cell number of the index



### h3_is_valid(`h3index`) ⇒ `bool`
*Since v1.0.0*


Returns true if the given H3Index is valid



### h3_is_res_class_iii(`h3index`) ⇒ `bool`
*Since v1.0.0*


Returns true if this index has a resolution with Class III orientation



### h3_is_pentagon(`h3index`) ⇒ `bool`
*Since v1.0.0*


Returns true if this index represents a pentagonal cell



### h3_get_faces(`h3index`) ⇒ `integer`
*Since v3.5.0*


Find all icosahedron faces intersected by a given H3 index

# Grid traversal functions

Grid traversal allows finding cells in the vicinity of an origin cell, and
determining how to traverse the grid from one cell to another.

### h3_k_ring(`h3index`, [k `integer` = 1]) ⇒ `h3index`
*Since v0.2.0*


Produces indices within "k" distance of the origin index



### h3_k_ring_distances(`h3index`, [k `integer` = 1], index `h3index`, distance `int`) ⇒ `record`
*Since v0.2.0*


Produces indices within "k" distance of the origin index paired with their distance to the origin



### h3_hex_ring(`h3index`, [k `integer` = 1]) ⇒ `h3index`
*Since v0.2.0*


Returns the hollow hexagonal ring centered at origin with distance "k"



### h3_line(`h3index`, `h3index`) ⇒ `h3index`
*Since v0.4.0*


Given two H3 indexes, return the line of indexes between them (inclusive).

This function may fail to find the line between two indexes, for
example if they are very far apart. It may also fail when finding
distances for indexes on opposite sides of a pentagon.



### h3_distance(`h3index`, `h3index`) ⇒ `integer`
*Since v0.2.0*


Returns the distance in grid cells between the two indices



### h3_experimental_h3_to_local_ij(origin `h3index`, index `h3index`) ⇒ `POINT`
*Since v0.2.0*


Produces local IJ coordinates for an H3 index anchored by an origin.
This function is experimental, and its output is not guaranteed to be compatible across different versions of H3.



### h3_experimental_local_ij_to_h3(origin `h3index`, coord `POINT`) ⇒ `h3index`
*Since v0.2.0*


Produces an H3 index from local IJ coordinates anchored by an origin.
This function is experimental, and its output is not guaranteed to be compatible across different versions of H3.

# Hierarchical grid functions

These functions permit moving between resolutions in the H3 grid system.
The functions produce parent (coarser) or children (finer) cells.

### h3_to_parent(`h3index`, [resolution `integer` = -1]) ⇒ `h3index`
*Since v1.0.0*


Returns the parent of the given index



### h3_to_children(`h3index`, [resolution `integer` = -1]) ⇒ `h3index`
*Since v1.0.0*


Returns the set of children of the given index



### h3_to_center_child(`h3index`, [resolution `integer` = -1]) ⇒ `h3index`
*Since v3.6.0*


Returns the center child (finer) index contained by input index at given resolution



### h3_compact(`h3index`) ⇒ `h3index`
*Since v0.2.0*


Compacts the given array as best as possible



### h3_uncompact(`h3index`, [resolution `integer` = -1]) ⇒ `h3index`
*Since v0.2.0*


Uncompacts the given array at the given resolution. If no resolution is given, then it is chosen as one higher than the highest resolution in the set




### h3_to_children_slow(index `h3index`, [resolution `integer` = -1]) ⇒ `h3index`
*Since v1.0.0*


Slower version of H3ToChildren but allocates less memory

# Region functions

These functions convert H3 indexes to and from polygonal areas.

### h3_polyfill(exterior `polygon`, holes `polygon`, [resolution `integer` = 1]) ⇒ `h3index`
*Since v0.2.0*


Takes an exterior polygon [and a set of hole polygon] and returns the set of hexagons that best fit the structure



### h3_set_to_multi_polygon(`h3index`, exterior `polygon`, holes `polygon`) ⇒ `record`
*Since v3.5.0*


Create a LinkedGeoPolygon describing the outline(s) of a set of hexagons. Polygon outlines will follow GeoJSON MultiPolygon order: Each polygon will have one outer loop, which is first in the list, followed by any holes

# Unidirectional edge functions

Unidirectional edges allow encoding the directed edge from one cell to a
neighboring cell.

### h3_indexes_are_neighbors(`h3index`, `h3index`) ⇒ `boolean`
*Since v1.0.0*


Returns true if the given indices are neighbors



### h3_get_h3_unidirectional_edge(origin `h3index`, destination `h3index`) ⇒ `h3index`
*Since v0.2.0*


Returns a unidirectional edge H3 index based on the provided origin and destination.



### h3_unidirectional_edge_is_valid(edge `h3index`) ⇒ `boolean`
*Since v1.0.0*


Returns true if the given edge is valid.



### h3_get_origin_h3_index_from_unidirectional_edge(edge `h3index`) ⇒ `h3index`
*Since v0.2.0*


Returns the origin index from the given edge.



### h3_get_destination_h3_index_from_unidirectional_edge(edge `h3index`) ⇒ `h3index`
*Since v0.2.0*


Returns the destination index from the given edge.



### h3_get_h3_indexes_from_unidirectional_edge(edge `h3index`, origin `h3index`, destination `h3index`) ⇒ `record`
*Since v0.2.0*


Returns the pair of indices from the given edge.



### h3_get_h3_unidirectional_edges_from_hexagon(`h3index`) ⇒ `h3index`
*Since v0.2.0*


Returns all unidirectional edges with the given index as origin



### h3_get_h3_unidirectional_edge_boundary(edge `h3index`) ⇒ `polygon`
*Since v0.2.0*


Provides the coordinates defining the unidirectional edge.

# Miscellaneous H3 functions

These functions include descriptions of the H3 grid system.

### h3_point_dist(a `point`, b `point`, [unit `text` = km]) ⇒ `float`
*Since v3.7.0*


The great circle distance in radians between two spherical coordinates.



### h3_hex_area(resolution `integer`, [unit `text` = km]) ⇒ `float`
*Since v3.5.0*


Average hexagon area in square (kilo)meters at the given resolution.



### h3_cell_area(cell `h3index`, [unit `text` = km^2]) ⇒ `float`
*Since v3.7.0*


Exact area for a specific cell (hexagon or pentagon).



### h3_edge_length(resolution `integer`, [unit `text` = km]) ⇒ `float`
*Since v3.5.0*


Average hexagon edge length in (kilo)meters at the given resolution.



### h3_exact_edge_length(edge `h3index`, [unit `text` = km]) ⇒ `float`
*Since v3.7.0*


Exact length for a specific unidirectional edge.



### h3_num_hexagons(resolution `integer`) ⇒ `bigint`
*Since v0.2.0*


Number of unique H3 indexes at the given resolution.



### h3_get_res_0_indexes() ⇒ `h3index`
*Since v1.0.0*


Returns all 122 resolution 0 indexes.



### h3_get_pentagon_indexes(resolution `integer`) ⇒ `h3index`
*Since v3.6.0*


All the pentagon H3 indexes at the specified resolution.

# Operators
## B-tree operators

### Operator: `h3index` = `h3index`
*Since v0.1.0*


Returns true if two indexes are the same



### Operator: `h3index` <> `h3index`
*Since v0.1.0*










## R-tree Operators

### Operator: `h3index` && `h3index`
*Since v3.6.1*


Returns true if the two H3 indexes intersect



### Operator: `h3index` @> `h3index`
*Since v3.6.1*


Returns true if A containts B



### Operator: `h3index` <@ `h3index`
*Since v3.6.1*


Returns true if A is contained by B


### Operator: `h3index` <-> `h3index`
*Since v3.7.0*


Returns the distance in grid cells between the two indices





# Extension specific functions

### h3_get_extension_version() ⇒ `text`
*Since v1.0.0*


Get the currently installed version of the extension.


# PostGIS Functions

### h3_geo_to_h3(`geometry`, resolution `integer`) ⇒ `h3index`
*Since v0.3.0*



### h3_geo_to_h3(`geography`, resolution `integer`) ⇒ `h3index`
*Since v0.3.0*



### h3_to_geometry(`h3index`) ⇒ `geometry`
*Since v1.0.0*



### h3_to_geography(`h3index`) ⇒ `geography`
*Since v1.0.0*



### h3_to_geo_boundary_geometry(`h3index`, [extend `BOOLEAN` = `false`]) ⇒ `geometry`
*Since v1.0.0*



### h3_to_geo_boundary_geography(`h3index`, [extend `BOOLEAN` = `false`]) ⇒ `geography`
*Since v1.0.0*



### h3_polyfill(multi `geometry`, resolution `integer`) ⇒ `h3index`
*Since v0.3.0*



### h3_polyfill(multi `geography`, resolution `integer`) ⇒ `h3index`
*Since v0.3.0*


## PostGIS casts
### `h3index` :: `point`
*Since v0.3.0*


### `h3index` :: `geometry`
*Since v0.3.0*


### `h3index` :: `geography`
*Since v0.3.0*



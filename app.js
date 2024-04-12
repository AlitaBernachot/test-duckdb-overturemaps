var duckdb = require('duckdb');
var db = new duckdb.Database(':memory:'); // or a file name for a persistent DB

db.all('INSTALL spatial;');
db.all('LOAD spatial;');
db.all("SET s3_region='us-west-2';");

//s3://overturemaps-us-west-2/release/2024-03-12-alpha.0/


db.all(`
COPY (
    SELECT
           type,
           subType,
           JSON(names) AS names,
           JSON(sources) AS sources,
           ST_GeomFromWkb(geometry) AS geometry
    FROM read_parquet('s3://overturemaps-us-west-2/release/2024-03-12-alpha.0/theme=admins/type=*/*', filename=true, hive_partitioning=1)
    WHERE ST_GeometryType(ST_GeomFromWkb(geometry)) IN ('POLYGON','MULTIPOLYGON')
    LIMIT 10
) TO 'countries.geojson'
WITH (FORMAT GDAL, DRIVER 'GeoJSON', SRS 'EPSG:4326');
`, function(err, res) {
  if (err) {
    console.warn("There was an error:", err);
    return;
  }
  console.log("result: ", res[0].fortytwo)
});
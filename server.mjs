import express from "express";
import cors from "cors";
import { doRequest, getFileName } from "./server-utils.mjs";

const PARQUET_PATH = '/home/abernachot/Dev/overture-map/data'; // or 's3://overturemaps-us-west-2/release/2024-04-16-beta.0'
const PARQUET_PATH_S3 = 's3://overturemaps-us-west-2/release/2024-04-16-beta.0'; // or 's3://overturemaps-us-west-2/release/2024-04-16-beta.0'
const APP_PORT = 3000;
const app = express();

app.use(cors());

/**
 * Home http://localhost:3000/
 */
app.get('/', async (req, res) => {
  const fileName = getFileName('test.geojson');
  const query = `
    SELECT
      type,
      subType,
      JSON(names) AS names,
      JSON(sources) AS sources,
      ST_GeomFromWkb(geometry) AS geometry,
      JSON(bbox) AS bbox
    FROM read_parquet('${PARQUET_PATH}/theme=admins/type=locality/*', filename=true, hive_partitioning=1)
    LIMIT 10000
  `;

  const data = await doRequest(query, fileName);
  res.send(data);
});

/**
 * Countries polygon http://localhost:3000/countries
 */
app.get('/countries', async (req, res) => {
  const fileName = getFileName('countries.geojson');
  const query = `
    SELECT
      admins.id,
      admins.subtype,
      admins.iso_country_code_alpha_2,
      names.primary AS primary_name,
      areas.area_id,
      ST_GeomFromWKB(areas.area_geometry) as geometry
    FROM read_parquet('${PARQUET_PATH}/theme=admins/type=*/*', filename=true, hive_partitioning=1)
      AS admins
    INNER JOIN (
        SELECT
            id as area_id,
            locality_id,
            geometry AS area_geometry
        FROM read_parquet('${PARQUET_PATH}/theme=admins/type=*/*', filename=true, hive_partitioning=1)
    ) AS areas ON areas.locality_id == admins.id
    WHERE admins.admin_level = 1
  `;

  const data = await doRequest(query, fileName);
  res.send(data);
});

/**
 * Places points http://localhost:3000/places
 * WARNING this query takes 5min to execute, better execute and create the file in cache on duckdb standalone before
 * thus adding LIMIT 10000 to the query
 */
app.get('/places', async (req, res) => {
  const fileName = getFileName('places.geojson');
  const query = `
    SELECT
      id,
      sources[1].dataset AS primary_source,
      names.primary AS primary_names,
      ST_GeomFromWkb(geometry) AS geometry
    FROM 
      read_parquet('${PARQUET_PATH}/theme=places/type=place/*', filename=true, hive_partitioning=1) places
    WHERE 
    	ST_Within(
	    	ST_GeomFromWKB(geometry), 
	    	ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":
          [[[54.4, 24.3],[54.8, 24.3],[54.8, 24.5],[54.4, 24.5],[54.4, 24.3]]]
        }')
    	)
    LIMIT 10000
  `;

  // [[[5.9559111595,45.8179931641],[10.4920501709,45.8179931641],[10.4920501709,47.808380127],[5.9559111595,47.808380127],[5.9559111595,45.8179931641]]]
  // [[[54.4, 24.3],[54.8, 24.3],[54.8, 24.5],[54.4, 24.5],[54.4, 24.3]]]

  const data = await doRequest(query, fileName);
  res.send(data);
});

app.listen(APP_PORT, () => console.log(`Server is running on port ${APP_PORT}`));
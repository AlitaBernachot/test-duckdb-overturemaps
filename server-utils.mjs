import duckdb from "duckdb";
import fs from "fs";

let db = new duckdb.Database(':memory:'); // or a file name for a persistent DB
db.all('INSTALL spatial;');
db.all('LOAD spatial;');
db.all("SET s3_region='us-west-2';");

export function getFileName(fileName) {
  return `./cache/${fileName}`;
}

export async function doRequest(sql, toFile = 'tmp.geojson') {
  // Wrap query to save to file
  sql = `COPY (${sql}) TO '${toFile}' WITH (FORMAT GDAL, DRIVER 'GeoJSON', SRS 'EPSG:4326');`;
  // Then, do Request if file does not exist
  if (!fs.existsSync(toFile)) await asyncHandleRequest(sql);
  // Finally, return file content
  return await fsReadFile(toFile);
}

function asyncHandleRequest(sql) {
  console.log(sql);
  return new Promise((resolve, reject) => db.all(sql, (err, res) => err ? reject(err) : resolve(res)));
}

function fsReadFile(fileName) {
  return new Promise((resolve, reject) => 
    fs.readFile(fileName, "utf8", 
      (err, data) => err ? reject(err) : resolve(data)));
};
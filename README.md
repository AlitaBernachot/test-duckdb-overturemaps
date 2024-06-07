# DuckDB x Overture Maps Geoparquet

How to use:

1. Install dependencies (`duckdb`)

```
$ npm install
```

2. Configure the server

```js
/** server.msj */
const PARQUET_PATH = '/yourpathto/overturemap/data';
// or for online direct query to Overture Map
const PARQUET_PATH = 's3://overturemaps-us-west-2/release/2024-04-16-beta.0';
const APP_PORT = 3000;
```

You will need to add a directory `/cache` to store requests results.

3. Launch the server

```
$ node server.mjs
```

4. Browse data

Go to `http://localhost:3000/countries` to get countries boundaries as geojson

This will create a `countries.geojson` with a collection of features.

5. Add more routes

You can create more routes with custom queries.

```js
/** server.mjs */
app.get('/my_new_route', async (req, res) => {
  const fileName = getFileName('mynewroute.json');
  const query = `
    ...complete query here...
  `;

  const data = await doRequest(query, fileName);
  res.send(data);
});
```
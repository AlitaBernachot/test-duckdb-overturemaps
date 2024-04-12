import express from "express";
import duckdb from "duckdb";

const app = express();
const port = 3000;

let db = new duckdb.Database(':memory:'); // or a file name for a persistent DB

app.get('/', (req, res) => {
    
    db.all('SELECT 42 AS fortytwo', function(err, res) {
    if (err) {
        console.warn("There was an error:", err);
        return;
    }
    console.log("result: ", res[0].fortytwo)
    });

  res.send('Welcome to my server!');
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
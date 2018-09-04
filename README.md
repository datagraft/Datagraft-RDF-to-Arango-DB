# datagraft-csv-to-arangodb
A Node.js service to transform CSV files to ArangoDB collections using [Grafterizer](https://grafterizer.datagraft.io/) RDF mappings.

## Installation and Usage

Install project dependencies using the [Node package manager](https://www.npmjs.com/).
```
npm install

```
Run the Express server. By default the server runs on port `3030` but this can be configured using environmental variables.
```
node server.js
```

### Environmental variables

```
HTTP_PORT <Server port. Default: 3030>
INPUT_STORAGE_LOCATION  <(temporary) Storage location for input CSV files. Default: './uploads/'>
RESULT_STORAGE_LOCATION <(tamporary) Storage location on disk where results of ArangoDB transformations should be stored. Default: './results/'>
```

### Request parameters


```
file: <CSV file to be used as input> (required)
mapping: <transformation JSON object from the Grafterizer user interface> (required)
```
<!-- /* If REST is set to true then these are required */

endpoint: <The Arango instence URL>
db: <The name for the database on arango to use>
name: <The collection name, that the valuse should be inserted to>
authToken: <The Bearer token aquired from Arango DB>
``` -->
### Usage

The endpoint is located at '/arango_transform/zip' (by default - http://localhost:3030/arango_transform/zip). The service returns a .zip file containing JSON files with the value and edge collections that can be imported to ArangoDB.


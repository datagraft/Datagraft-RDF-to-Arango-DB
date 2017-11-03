# Datagraft-RDF-to-Arango-DB
A node script to transform datagraft RDF mappings to Arango DB values. But this branch takes a CSV and maps each line to a object that can be imported into Arango DB

## The service
All you need to do to run this service is clone this branch and run these four commands:

```
1: mkdir uploads
2: mkdir result
3: npm install
4: npm run 
```

This will expose the service on port `3030`

### Avaiable request parameters
Paramaeters enclosed by `<>` is required, parameters enclosed in `[]`ar optionals

```
csv: <csv file>
REST: [true/false]

/* If REST is set to true then these are required */
endpoint: <The Arango instence URL>
db: <The name for the database on arango to use>
name: <The collection name, that the valuse should be inserted to>
authToken: <The Bearer token aquired from Arango DB>
```

### Getting the authToken
To get the auth token from arango db, you'll need to do a post call to: `<your arango url>/_open/auth`with a JSON object containing a username and password.
```
{
    'username': 'user',
    'password': 'pass'
}
```

## transformscript.js
This is the file doing the transformation and inserting/saving the result.

### REST
When using the REST option to insert the data. The transformed result is sent to the specified arango instence.

### Saving result
When not using REST the result gets saved to file in the
subdirectory `result/` at the same location as the script is located. This can then be imported into Arango DB manually throug theire `arangoimp` function.

## How to import to ArangoDB - when using Docker
Run these lines of code to copy the file to the docker container, and import values into Arango.

To copy the file:

```docker cp arango_value.json <your image name>:/arangovalue.json```

To import and make new collection

```docker exec -i arangodb arangoimp --file arangovalue.json --collection <collection name> --create-collection true```

### importing the csv directly throug comandline
```docker cp csvdocument.csv arangodb:/csvdocument.csv```

```docker exec -i arangodb arangoimp --file csvdocument.csv --type csv --collection test_csv --create-collection true```

## Test files
There is one csv file located at `test/`
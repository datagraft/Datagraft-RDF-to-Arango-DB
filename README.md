# Datagraft-RDF-to-Arango-DB
A node script to transform datagraft RDF mappings to Arango DB values

# REST.js
First you'll need to obtain an token from DC/OS follow this link for how to obtain it: [Get token](https://docs.mesosphere.com/1.9/security/iam-api/#obtaining-an-authentication-token)

Then insert the token into the value `authToken` in the `REST.js` file.
Also add your database endpoint to the value `connection` in the same file. 

NB! this is baste on doing AJAX calls and requiers that the application has acces to jquery

## How to use the script
To run the script localy start a comand-window to run node localy

1. Add / Edit the RDF mapping values, and vocabulary for the RDF mapping. The RDF mapping is in the file ```rdfMapping.js``` and the vocab in ```rdfVocab.js```
2. load the ```transformscript.js``` into Node. (In node repl it can be done with `.load transformscript.js`)
3. Read the csv file with ```read([path_to_file]);``` specifying the file path as input.
4. Build a list of haddings with ```build();``` this is used to get the propper colum when inserting data trhought the transformation
5. Now run the mapping function with ```run();``` this should start the entire process and outputs a .json file for edges and for values corresponding to the arangoformat.

The json files has one object per line, therby noe filesize limitation when using the arango import function

## How to import to ArangoDB
Run these lines of code to copy the files to the docker container, and import values into Arango.

``` NB! substitut "arangodb" with the name of your docker instance ``` 

To copy the files:

```docker cp arango_value.json arangodb:/arangovalue.json```

```docker cp arango_edge.json arangodb:/arangoedge.json```

To import and make new collection (exchange "test" / "test_edge" with the collection name)

```docker exec -i arangodb arangoimp --file arangovalue.json --collection test --create-collection true```

```docker exec -i arangodb arangoimp --file arangoedge.json --collection test_edge --create-collection true --create-collection-type edge --from-collection-prefix test --to-collection-prefix test```

### importing the csv directly (not makin a graph)
```docker cp csvdocument.csv arangodb:/csvdocument.csv```

```docker exec -i arangodb arangoimp --file csvdocument.csv --type csv --collection test_csv --create-collection true```

## Future improvments
- Implementing a csv parser, so we ensure it is not braking on bad formatting / different formatting than whats coded.
- Run the mapping per line while reading the csv, don't read everything in to memory before transforming.
- Inserting information into the DB with the REST API inserting information on a stream basis?

const $ = require('jquery');

/*The URL path to the database*/
const connection = '';

/*The auth key for DC/OS*/
const authToken = ``;

/*Get document
* id = collectionName/_key
* [optional] database = databasename [default is _system]
*/
function getDocument(id, database = '_system'){
  $.ajax({
    "async": true,
    "crossDomain": true,
    "url": connection + database + "/_api/document/" + id,
    "method": "GET",
    "headers": {
      "authorization": "token=" + authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "processData": false
  }).done(function (response) {
    console.log(response);
  });
};

/*Insert document
* body = json object to be inserted
* [optional] database = databasename [default is _system]
*/
function insertDocument(body, database = '_system'){
  $.ajax({
    "async": true,
    "crossDomain": true,
    "url": connection + database + "/_api/document/" + id,
    "method": "POST",
    "headers": {
      "authorization": "token=" + authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "processData": false,
    "data" : body
  }).done(function (response) {
    console.log(response);
  });
};

/* query the database
*  body = json object containing at least
*  the key 'query' with a text of AQL query
*  
*  including the key 'batchSize' in the body object and setting it
*  to an number will limit the number of results returned.
*
*  An indicator in the result 'hasMore' is a true/false value
*  telling if there's more to get.
*  To get the next batch of results use tje 'id' together with
*  the function getNextBatch
*
*  [optional] database = databasename [default is _system]
* 
*/
function query(body, database = '_system'){
  $.ajax({
    "async": true,
    "crossDomain": true,
    "url": connection + database + "_api/cursor/",
    "method": "POST",
    "headers": {
      "authorization": "token=" + authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "processData": false,
    "data" : body
  }).done(function (response) {
    console.log(response);
  });
};

/*Get next batch
* id = the ID returned from a resultset of a query
*
* [optional] database = databasename [default is _system]
*
*/
function getNextBatch(id, database = '_system'){
  $.ajax({
    "async": true,
    "crossDomain": true,
    "url": connection + database + "_api/cursor/" + id,
    "method": "PUT",
    "headers": {
      "authorization": "token=" + authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "processData": false,
  }).done(function (response) {
    console.log(response);
  });
};

/* Explain a query
*  body = json object containing 
*  the key 'query' with a text of AQL query
*
* [optional] database = databasename [default is _system]
*/
function explain(body, database = '_system'){
  $.ajax({
    "async": true,
    "crossDomain": true,
    "url": connection + database + "_api/explain/",
    "method": "POST",
    "headers": {
      "authorization": "token=" + authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "processData": false,
    "data": body//"{ \n\t\"query\" : \"FOR b IN buildings FILTER b.prefix == \\\"Building\\\" RETURN b\",\n\t\"count\" : true, \n\t\"batchSize\" : 2\n\t}"
  }).done(function (response) {
    console.log(response);
  });
};

/*Create collection
* body = a JSON object with minimum a key 'name' that's the collection name.
* add the key 'type' and set the value to '3' to create an edge collection
*
* [optional] database = databasename [default is _system]
*/
function createCollection(body, database = '_system'){
  $.ajax({
    "async": true,
    "crossDomain": true,
    "url": connection + database + "/_api/collection/",
    "method": "POST",
    "headers": {
      "authorization": "token=" + authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "processData": false,
    "data": body//"{ \n\t\"query\" : \"FOR b IN buildings FILTER b.prefix == \\\"Building\\\" RETURN b\",\n\t\"count\" : true, \n\t\"batchSize\" : 2\n\t}"
  }).done(function (response) {
    console.log(response);
  });
};

/*List collections
* [optional] database = databasename [default is _system]
*/
function listCollections(database = '_system'){
  $.ajax({
    "async": true,
    "crossDomain": true,
    "url": connection + database + "/_api/collection/",
    "method": "GET",
    "headers": {
      "authorization": "token=" + authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "processData": false
  }).done(function (response) {
    console.log(response);
  });
};

/*List database*/
function listDatabases(){
  $.ajax({
    "async": true,
    "crossDomain": true,
    "url": connection + "_api/database/",
    "method": "GET",
    "headers": {
      "authorization": "token=" + authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "processData": false
  }).done(function (response) {
    console.log(response);
  });
};

/*Create database
* body = a JSON object with at least the key 'name' with the name of the DB to create
*/
function createDatabase(body){
  $.ajax({
    "async": true,
    "crossDomain": true,
    "url": connection + "_api/database/",
    "method": "POST",
    "headers": {
      "authorization": "token=" + authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "processData": false,
    "data": body//"{ \n\t\"query\" : \"FOR b IN buildings FILTER b.prefix == \\\"Building\\\" RETURN b\",\n\t\"count\" : true, \n\t\"batchSize\" : 2\n\t}"
  }).done(function (response) {
    console.log(response);
  });
};


/*delete database
* body = a JSON object with the key 'name' with the name of the DB to delete.
*/
function deleteDatabase(body){
  $.ajax({
    "async": true,
    "crossDomain": true,
    "url": connection + "_api/database/",
    "method": "POST",
    "headers": {
      "authorization": "token=" + authToken,
      "content-type": "application/json",
      "cache-control": "no-cache",
    },
    "processData": false,
    "data": body//"{ \n\t\"query\" : \"FOR b IN buildings FILTER b.prefix == \\\"Building\\\" RETURN b\",\n\t\"count\" : true, \n\t\"batchSize\" : 2\n\t}"
  }).done(function (response) {
    console.log(response);
  });
};
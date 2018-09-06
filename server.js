/**
 * Datagraft-RDF-to-Arago-DB transforms DataGraft RDF mappings to Arango DB values
 */

var express = require('express')
var zip = require('express-zip');
var cors = require('cors')
const logging = require('./logging');
//require('./transformscript');
var bodyParser = require('body-parser');
const request = require('request');
const serverPort = process.env.HTTP_PORT || 3030;
const resultStorageLocation = process.env.RESULT_STORAGE_LOCATION || './results/';
const inputStorageLocation = process.env.INPUT_STORAGE_LOCATION || './uploads/';

const app = express();
app.use(bodyParser.json({limit: '50mb'}));

app.use(cors());

app.get('/', (req, res) => {
  res.send('ok');
});

require('./transformscript')(app, {
  resultStorageLocation,
  inputStorageLocation
});

app.listen(serverPort, () => {
  logging.info('Datagraft-CSV-to-Arago-DB started on http://localhost:' + serverPort + '/');
});

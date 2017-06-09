/**
 * Datagraft-RDF-to-Arago-DB transforms DataGraft RDF mappings to Arango DB values
 */

// Express is a HTTP server library.
var express = require('express')

// HTTP CORS library, to allow other applications domains
// to contact this service.
var cors = require('cors')

// Logging component for error and info messages
const logging = require('./logging');

// This is the TCP port where this component is listening
const serverPort = process.env.HTTP_PORT || 8089;

const app = express();

var corsOptions = {
  origin: 'http://localhost:9000',
  optionsSuccessStatus: 200 // some legacy browsers (IE11, various SmartTVs) choke on 204
}

app.get('/products/:id', cors(corsOptions), function (req, res, next) {
  res.json({msg: 'This is CORS-enabled for only localhost:9000'})
})


 
app.listen(serverPort, () => {
  logging.info('Datagraft-RDF-to-Arago-DB started on http://localhost:' + serverPort + '/');
});

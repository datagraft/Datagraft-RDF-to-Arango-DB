/* load the rdf mapping and vocab into the script */
//.load rdfMapping.js
//.load rdfVocab.js
const logging = require('./logging');
const multer = require('multer');
const parser = require('./papaparse.min.js');
const fs = require('fs');
const rest = require('./REST.js');

/** Hash function - used to hash fully qualified names for ArangoDB keys
 */
const hashCode = (str) => {
  var hash = 0;
  if (str.length === 0) return hash;
  for (i = 0; i < str.length; i++) {
    char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  return (hash + 2147483647) + 1;
};


module.exports = (app, settings) => {

  const storage = multer.diskStorage({
    destination: function (req, file, callback) {
      callback(null, settings.inputStorageLocation);
    },
    filename: function (req, file, callback) {
      callback(null, file.fieldname + '-' + Date.now());
    }
  });
  const upload = multer({ storage: storage });
  const cpUpload = upload.fields([{ name: 'file', maxCount: 1 }, { name: 'mapping', maxCount: 1 }]);

  var arangoPrefixes = {},
    stamp,
    arangoValuesFilePath,
    arangoEdgeFilePath,
    // Variables that will be used to write streams to the files for the value and edge collections
    wsValues,
    wsEdges,
    incompleteMappingWarning = false,
    valuesNotFoundWarning = false,
    mappedUriNodesMap = new Map(),
    vocabularyMapping = [];

  /** Adds the prefix mapping to a global variable. 
 * Prefixes are appended to the ArangoDB JSON in the end of the JSON generation process.
 * @param  {} prefix - prefix name
 */
  const addPrefixMapping = (prefix) => {
    var namespace = vocabularyMapping[prefix].namespace;

    var nameHash = hashCode(namespace).toString();
    var alreadyMappedPrefix = false;
    for (var pref in arangoPrefixes) {
      if (pref === nameHash) {
        alreadyMappedPrefix = true;
      }
    }

    if (!alreadyMappedPrefix && nameHash !== undefined) {
      arangoPrefixes[nameHash] = {
        _key: nameHash,
        namespaceURI: namespace,
        prefix: prefix,
        type: ['Prefix']
      };
    }

  }

  /** Checks if two URI nodes that are added to the mapping are the same
   * @param  {} uriNode1 first node for the comparison
   * @param  {} uriNode2 second node for the comparison
   * @param  {} line line of input that is currently being mapped
   */
  const isSameUriNode = (uriNode1, uriNode2, line, headings, vocabularyMapping) => {
    if (uriNode1.__type === uriNode2.__type) {
      // Compare the fully qualified names
      switch (uriNode1.__type) {
        case 'ColumnURI':
          var columnValue1 = line[headings.indexOf(uriNode1.column.value)];
          var columnValue2 = line[headings.indexOf(uriNode2.column.value)];
          // If fully qualified names are the same - the nodes are the same
          if (vocabularyMapping[uriNode1.prefix].namespace + columnValue1 === vocabularyMapping[uriNode2.prefix].namespace + columnValue2) {
            return true;
          } else {
            return false;
          }
          break;
        case 'ConstantURI':
          var constantValue1 = uriNode1.constant;
          var constantValue2 = uriNode2.constant;
          if (vocabularyMapping[uriNode1.prefix].namespace + constantValue1 === vocabularyMapping[uriNode2.prefix].namespace + constantValue2) {
            return true;
          } else {
            return false;
          }
          break;
        default:
          // Neither is a URI node
          return false;
      }
    } else {
      return false;
    }
  }

  /**
   * Maps a property node to the right ArangoDB JSON format using the following mapping rules:
   * 
   *  -- All literal objects in the RDF mapping will be mapped to JSON object attributes (except the two following cases) 
   *  -- All rdf:type property values in the RDF mapping will be stored as 'type' attributes
   *  -- All rdfs:label property values will be stored as 'label' attributes
   * 
   * @param  {} property property to be mapped
   * @param  {} rootMapping current root mapping object
   * @param  {} graphMapping current graph mapping object
   * @param  {} line line that is currently being mapped
   * @param  {} arangoValues list of ArangoDB JSON objects for the value collection
   * @param  {} arangoEdges list of ArangoDB JSON objects for the edge collection
   */
  const mapProperty = (property, rootMapping, graphMapping, line, arangoValues, arangoEdges, headings) => {
    if (property.prefix) {
      // TODO - we may be adding prefixes that are not referenced here! This is because we do not have literal properties as nodes themselves
      addPrefixMapping(property.prefix, arangoEdges);
    }
    switch (property.prefix ? (vocabularyMapping[property.prefix].namespace + property.propertyName) : property.propertyName) {
      case 'http://www.w3.org/2000/01/rdf-schema#label':
        var labelNode = property.subElements[0];
        if (labelNode.__type === 'ColumnLiteral') {
          // The label of the node mapping is mapped to the value of the column
          rootMapping.label = line[headings.indexOf(labelNode.literalValue.value)];
        } else if (labelNode.__type === 'ConstantLiteral') {
          // The label of the node mapping is just a free-defined string
          rootMapping.label = labelNode.literalValue.value;
        } else {
          // Invalid RDF mapping - ignore this (blank nodes or URI nodes should not be mapped as labels!)
          console.log("WARNING: wrong RDF mapping - blank nodes or URI nodes should not be mapped to rdfs:label-s! Ignoring the label mapping...");
        }
        break;
      case 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type':
        var typeNode = property.subElements[0];
        if (typeNode.__type === 'ColumnURI') {
          // Add the prefix to the prefix mapping and add the qualified name as value to the type attribute in the resulting object
          if (typeNode.prefix) {
            addPrefixMapping(typeNode.prefix, arangoEdges);
          }
          // Add type attribute to the root mapping - fully qualified URI (http://...#<prefix_name>)
          rootMapping.type.push((typeNode.prefix ? vocabularyMapping[typeNode.prefix].namespace : '')
            + line[headings.indexOf(typeNode.column.value)]);
        } else if (typeNode.__type === 'ConstantURI') {
          // Add the prefix to the prefix mapping and add the qualified name as value to the type attribute in the resulting object
          if (typeNode.prefix) {
            addPrefixMapping(typeNode.prefix, arangoEdges);
          }
          // Type mapped to either <prefix>:<value> or just <value> if no prefix defined
          rootMapping.type.push((typeNode.prefix ? vocabularyMapping[typeNode.prefix].namespace : '') + typeNode.constant);
        } else {
          // Invalid RDF mapping - ignore this (blank nodes or literal nodes should not be mapped as types!)
          console.log("WARNING: wrong RDF mapping - blank nodes or literal nodes should not be mapped to rdf:type-s! Ignoring the type mapping...");
        }
        break;
      default:
        // All other literals will be added to the object; URI nodes will be added separately based on the graph mapping;
        // first - check if the object is a URI node
        switch (property.subElements[0].__type) {
          case 'ConstantURI':
          case 'ColumnURI':
            var uriObject = property.subElements[0];
            var foundUriNodeNotYetMapped = false;
            var comparedRoot = {};

            // Go through the root nodes in the mapping
            for (var j = 0; j < graphMapping.graphRoots.length; ++j) {
              comparedRoot = graphMapping.graphRoots[j];
              // We search for URI nodes in the roots that are mapped to the same URI 
              if (isSameUriNode(uriObject, comparedRoot, line, headings, vocabularyMapping) && !comparedRoot.mappingToArango) {
                foundUriNodeNotYetMapped = true;
                break;
              }
            }
            var uriObjectMapping = {};

            // If the URI node has not yet been mapped - map it in order to get the ID to add antry to the Edge collection.
            // The entry connects the currently mapped root to the matched node that was just mapped.
            if (foundUriNodeNotYetMapped) {
              uriObjectMapping = mapURINode(comparedRoot, graphMapping, line, arangoValues, arangoEdges, headings);
            } else {
              // Need to create a new node and a link in the edge collection
              uriObjectMapping = mapURINode(uriObject, graphMapping, line, arangoValues, arangoEdges, headings);
            }
            arangoEdges.push({
              "_from": rootMapping._key,
              "_to": uriObjectMapping._key,
              rdf: property.prefix ? (vocabularyMapping[property.prefix].namespace + property.propertyName) : property.propertyName
            });

            // Constant URI nodes could be isolated nodes - we would like to avoid adding them more than once to the arango mapping

            break;
          case 'BlankNode':
            // Blank nodes mapped to objects in the resulting root mapping object- should be only literals + maybe type!
            // We only map 1-level-depth blank node descriptions (<Graph root node> -> <Property> -> <Blank node> -> <Properties [1..x]> -> <Literal values/Types [1..x]>)
            break;
          default:
            // Literals are mapped to attributes to the new objects; the attribute names are taken from the RDF mapping
            var attributeMappingName = (property.prefix ? property.prefix + ':' : '') + property.propertyName;
            if (property.subElements[0].__type == 'ConstantLiteral') {
              var attributeMappingValue = property.subElements[0].literalValue;
            } else if (property.subElements[0].__type == 'ColumnLiteral') {
              var attributeMappingValue = line[headings.indexOf(property.subElements[0].literalValue.value)];
            } else {
              // Should not be possible
              console.log('WARNING: unexpected mapping type found: ' + property.subElements[0].__type);
            }
            rootMapping[attributeMappingName] = attributeMappingValue;
            break;
        }
        break;

    }
  }

  /**
   * Maps a line according to graph roots definition in a graph mapping and adds the result to the collections for ArangoDB values and edges
   * @param  {} uriNode URI node to be mapped
   * @param  {} graphMapping current graph mapping object
   * @param  {} line line that is currently being mapped
   * @param  {} arangoValues list of ArangoDB JSON objects for the value collection
   * @param  {} arangoEdges list of ArangoDB JSON objects for the edge collection
   */
  const mapURINode = (uriNode, graphMapping, line, arangoValues, arangoEdges, headings) => {
    var blankNodeCounter = 0;
    uriNode.mappingToArango = true;
    var uriNodeMapping = {
      type: []
    };
    if (uriNode.__type === 'ColumnURI' || uriNode.__type === 'ConstantURI') {
      // URI root - continue as planned
      if (uriNode.__type === 'ColumnURI') {
        // Get column value from the line
        var columnValue = line[headings.indexOf(uriNode.column.value)];
        if (!columnValue) {
          if (!valuesNotFoundWarning) {
            console.log('WARNING: Some of the column values were not found for the provided mapping!');
            valuesNotFoundWarning = true;
          }
        } else {
          uriNodeMapping.value = columnValue;
          // Add the prefix if it is available and the RDF value
          if (uriNode.prefix) {
            uriNodeMapping.prefix = uriNode.prefix;
            addPrefixMapping(uriNode.prefix, arangoEdges);
            // RDF value is the fully qualified name (URI of prefix and column value together)
            uriNodeMapping.rdf = vocabularyMapping[uriNode.prefix].namespace + columnValue;
          } else {
            // Value should be a full URI - just add the column value
            uriNodeMapping.rdf = columnValue;
          }
        }
      } else if (uriNode.__type === 'ConstantURI') {
        var constantValue = uriNode.constant;
        if (!constantValue) {
          if (!valuesNotFoundWarning) {
            console.log('WARNING: Some of the column values were not found for the provided mapping!');
            valuesNotFoundWarning = true;
          }
        } else {
          uriNodeMapping.value = constantValue;
          // Add the prefix if it is available and the RDF value
          if (uriNode.prefix) {
            uriNodeMapping.prefix = uriNode.prefix;
            addPrefixMapping(uriNode.prefix, arangoEdges);
            // RDF value is the fully qualified name (URI of prefix and column value together)
            uriNodeMapping.rdf = vocabularyMapping[uriNode.prefix].namespace + constantValue;
          } else {
            // Value should be a full URI - just add the column value
            uriNodeMapping.rdf = constantValue;
          }
        }
      }
      // If we did not manage to specify the rdf of a URI node, that means that there were probably missing information
      // in the input data; such URI nodes should be ignored
      if (uriNodeMapping.rdf) {
        // Add the mapping of the root to the Arango values if we already mapped this URI node
        if (!mappedUriNodesMap.has(hashCode(uriNodeMapping.rdf).toString())) {
          arangoValues.push(uriNodeMapping);
          mappedUriNodesMap.set(hashCode(uriNodeMapping.rdf).toString(), true);
        }
        uriNodeMapping._key = hashCode(uriNodeMapping.rdf).toString();
      } else {
        if (!incompleteMappingWarning) {
          incompleteMappingWarning = true;
          console.log("WARNING: incomplete mapping for object detected!");
        }
      }

      // Go through the properties of the graph root
      for (var i = 0; i < uriNode.subElements.length; ++i) {
        mapProperty(uriNode.subElements[i], uriNodeMapping, graphMapping, line, arangoValues, arangoEdges, headings);
      }

    } else {
      // TODO Literal root node - strange, but OK!

      // give it an ID
      // add to the collection of stuff to add to arango
    }



    // delete the root being mapped from the mapping

    // return the mapping key - may be needed for the edge collection
    return uriNodeMapping;
  }

  /**
   * Maps a line of input according to a graph mapping using the following rules:
   * 
   * -- URI nodes are mapped as documents in a value collection
   * -- All literals are mapped to attributes (except rdf:label-s and rdf:type-s)
   * 
   * @param  {} graphMapping current graph mapping object
   * @param  {} line line that is currently being mapped
   * @param  {} arangoValues list of ArangoDB JSON objects for the value collection
   * @param  {} arangoEdges list of ArangoDB JSON objects for the edge collection
   */
  const mapFlat = (graphMapping, line, arangoValues, arangoEdges, headings) => {
    for (i = 0; i < graphMapping.graphRoots.length; ++i) {
      var graphRoot = graphMapping.graphRoots[i];
      // If we have not yet mapped the root, mark the node and map it
      if (!graphMapping.graphRoots[i].mappingToArango) {
        mapURINode(graphRoot, graphMapping, line, arangoValues, arangoEdges, headings);
      }
    }
    // Reset the graph mapping object
    for (i = 0; i < graphMapping.graphRoots.length; ++i) {
      graphMapping.graphRoots[i].mappingToArango = false;
    }
  }


  const mapToArangoDB = (input_path, values_path, edges_path, graph_mapping, response_object, use_rest) => {
    var headings = [],
      arango_value = [], // ArangoDB value collection array
      arango_edge = [], // ArangoDB edge collection array
      createdHeadings = false,
      lineReader = require('readline').createInterface({ //Start new line reader
        input: fs.createReadStream(input_path),
        terminal: false
      }),
      line_counter = 0;
    console.time("transformData");

    // Open the write stream objects only after the line reader has been initialised successfully
    wsValues = fs.createWriteStream(values_path);
    wsEdges = fs.createWriteStream(edges_path);

    //On each line map corresponding heading to value
    //in a new JSON object
    lineReader.on('line', function (line) {
      line = parser.parse(line, {
        dynamicTyping: true
      });
      ++line_counter;
      if (!createdHeadings) {
        createdHeadings = true;
        headings = line.data[0];
      } else {
        if (!line.data[0]) {
          // Apparently sometimes there are random empty lines in the input files...
          console.log("Empty or corrupted line found - ignoring ...");
          console.log(line);
          console.log("Line number: " + line_counter + ". Line text: ");
          console.log(lineBak);
        } else {
          mapFlat(graph_mapping, line.data[0], arango_value, arango_edge, headings);
          write_object(arango_value, arango_edge);
        }
      }
    });

    lineReader.on('close', function () {
      console.log(arangoPrefixes);

      for (var key in arangoPrefixes) {
        if (arangoPrefixes.hasOwnProperty(key)) {
          fs.appendFileSync(values_path, JSON.stringify(arangoPrefixes[key]) + '\n');
        }
      }

      if (use_rest) {
        // TODO test? this!
        rest.insertDocument(JSON.stringify(arango_value), name, db, endpoint, authToken)
          .then(console.log("Inserted nodes to collection: " + name))
          .catch(res => console.log(res));
      }

      console.log("Done!");
      console.timeEnd("transformData");
      console.log("Lines: " + line_counter);

      // Close streams
      wsValues.end();
      wsEdges.end();

      response_object.zip([
        { path: arangoValuesFilePath, name: 'arango_values.json' },
        { path: arangoEdgeFilePath, name: 'arango_edges.json' }
      ], "arango_transformed.zip");

    });

    // writer function. Writes on arango object per line, and
    // outputs the file to the sub directory "results"
    function write_object(arangoValue, arangoEdge) {
      // Write nodes
      for (var i = 0; i < arangoValue.length; i++) {
        wsValues.write(JSON.stringify(arangoValue[i]) + '\n');
      }

      // Write edges
      for (i = 0; i < arangoEdge.length; i++) {
        wsEdges.write(JSON.stringify(arangoEdge[i]) + '\n');
      }
    }

    //When the file is closed (we have read the last line)
    //Write to file or insert using REST API
    // lineReader.on('close', function () {
    //   /*  If the option to use REST to insert is specified
    //   *   Insert to arango DB using the provided arango information
    //   *   Else write the result to file.
    //   */
    //   if (useRest) {
    //     rest.insertDocument(JSON.stringify(arango_value), name, db, endpoint, authToken)
    //       .then(console.log("Inserted nodes to collection: " + name))
    //       .catch(res => console.log(res));
    //   } else {
    //     write_object(arango_value);
    //   }
    //   res.send('ok');
    // });
  }


  app.post('/', cpUpload, (req, res) => {
    // console.log(req.body);
    incompleteMappingWarning = false,
      valuesNotFoundWarning = false,
      mappedUriNodesMap = new Map(),
      vocabularyMapping = [];

    // generate resulting file name based on current time
    stamp = Date.now();
    arangoValuesFilePath = settings.resultStorageLocation + stamp + "_arango_value.json";
    arangoEdgeFilePath = settings.resultStorageLocation + stamp + "_arango_edge.json";


    const showAndLogError = (res, status, message, data) => {
      // If the headers are already sent, it probably means the server has started to
      // provide a message and it's better to just keep the same message instead of
      // crashing trying to send already sent headers
      if (!res.headersSent) {
        res.status(status).json({
          error: message,
          data
        });
      }

      logging.error(message, data);
    };

    const inputFile = req.files.file[0];
    const inputFilePath = req.files.file[0].path;
    const transformation = JSON.parse(req.body.mapping);

    // merge graph mapping arrays (there could be two or more according to the Transformation Data Model of Grafterizer)
    const graph_mappings = transformation.extra.graphs;
    // we only need the graphRoots for the mapping
    var graphMapping = { graphRoots: [] };

    for (graph of graph_mappings) {
      graphMapping.graphRoots = graphMapping.graphRoots.concat(graph.graphRoots);
    }
    // extract RDF vocabularies from the transformation object
    transformation_vocabs = transformation.extra.rdfVocabs;

    /* Make vocabs for dataset */
    vocabularyMapping = [];
    for (var i = 0; i < transformation_vocabs.length; i++) {
      var key = transformation_vocabs[i].name;
      vocabularyMapping[key] = transformation_vocabs[i];
    }

    // const graph_mapping = Array.prototype.concat.apply([], JSON.parse(req.body.mapping).extra.graphs);

    //Params for REST calls
    const useRest = req.body.REST;
    const endpoint = req.body.endpoint;
    const db = req.body.db;
    const name = req.body.name;

    const authToken = req.body.authToken;

    if (!inputFile) {
      showAndLogError(res, 400, 'The source csv is missing');
      return;
    }

    if (useRest) {
      // storage directly in ArangoDB through the exposed REST service
      if (!endpoint) {
        showAndLogError(res, 400, 'Missing endpoint path');
        return;
      }
      if (!db) {
        showAndLogError(res, 400, 'Missing db name');
        return;
      }
      if (!name) {
        showAndLogError(res, 400, 'Missing a name for the dataset / transformation');
        return;
      }

      rest.listCollections(name, db, endpoint, authToken)
        .then(result => {
          console.log(result);

          mapToArangoDB(inputFilePath, arangoValuesFilePath, arangoEdgeFilePath, graphMapping, res);
        }).catch(reason => {
          //We didn't have the collection try to create them
          console.log(reason);
          //create the collection
          rest.createCollection(name, 1, db, endpoint, authToken)
            .then(result => {
              //console.log(result)
              mapToArangoDB(inputFilePath, arangoValuesFilePath, arangoEdgeFilePath, graphMapping, res);
            })
            .catch(result => showAndLogError(res, 500, 'could not create collection'));
        });
    } else {
      lineCounter = 0;
      mapToArangoDB(inputFilePath, arangoValuesFilePath, arangoEdgeFilePath, graphMapping, res);
    }


  });
};
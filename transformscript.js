/* jshint esversion: 6 */
const pa = require('./papaparse.min.js');
const dl = require('datalib');
const fs = require('fs');

/* Containers for transformation data */
var vocabularyMapping = [],
    headings = {},
    buffer,
    countToBeRemoved = 0,
    arangoPrefixes = {},
    args = require('minimist')(process.argv.slice(2)),
    transformation_vocabs = [],
    graph_mapping = {},
    lineCounter = 0,
    constants = {},
    valuesNotFoundWarning = false,
    incompleteMappingWarning = false,
    mappedUriNodesMap = new Map();


/* Hash function - used to hash namespaces for keys */
String.prototype.hashCode = function () {
  var hash = 0;
  if (this.length === 0) return hash;
  for (i = 0; i < this.length; i++) {
    char = this.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  return (hash + 2147483647) + 1; 
};

/* Read function of the csv file */


function addPrefixMapping(prefix, arangoEdges) {
  var namespace = vocabularyMapping[prefix].namespace;

  var nameHash = namespace.hashCode().toString();
  var alreadyMappedPrefix = false;
  for(var pref in arangoPrefixes){
    if (pref === nameHash) {
      alreadyMappedPrefix = true;
    }
  }

  if (!alreadyMappedPrefix && nameHash !== undefined){
    arangoPrefixes[nameHash] = {
      _key: nameHash, 
      namespaceURI: namespace,
      prefix: prefix,
      type: 'Prefix'
    };
  }

}

function isSameUriNode(uriNode1, uriNode2, line) {
  if (uriNode1.__type === uriNode2.__type) {
    // Compare the fully qualified names
    switch (uriNode1.__type) {
      case 'ColumnURI':
        var columnValue1 = line[headings[uriNode1.column.value]];
        var columnValue2 = line[headings[uriNode2.column.value]];
        // If fully qualified names are the same - the nodes are the same
        if (vocabularyMapping[uriNode1.prefix].namespace + columnValue1 ===  vocabularyMapping[uriNode2.prefix].namespace + columnValue2) {
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
        break;
    }
  } else {
    return false;
  }

  return false;
}

function mapProperty(property, rootMapping, graphMapping, line, arangoValues, arangoEdges) {
  if(property.prefix) {
    // TODO - we may be adding prefixes that are not referenced here! This is because we do not have literal properties as nodes themselves
    addPrefixMapping(property.prefix, arangoEdges);
  }
  /*

   All literal objects in the RDF mapping will be mapped to JSON object attributes; 
   All rdf:type property values in the RDF mapping will be stored as 'type' attributes; 
   All rdfs:label property values will be stored as 'label' attributes;

  */
  switch (property.prefix ? (vocabularyMapping[property.prefix].namespace + property.propertyName) : property.propertyName) {
    case 'http://www.w3.org/2000/01/rdf-schema#label':
      var labelNode = property.subElements[0];
      if (labelNode.__type === 'ColumnLiteral') {
        // The label of the node mapping is mapped to the value of the column
        rootMapping.label = line[headings[labelNode.literalValue.value]];
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
        if(typeNode.prefix) {
          addPrefixMapping(typeNode.prefix, arangoEdges);
        }
        // Add type attribute to the root mapping - fully qualified URI (http://...#<prefix_name>)
        rootMapping.type.push((typeNode.prefix ? vocabularyMapping[property.prefix].namespace : '') 
                              + line[headings[typeNode.column.value]]);
      } else if (typeNode.__type === 'ConstantURI') {
        // Add the prefix to the prefix mapping and add the qualified name as value to the type attribute in the resulting object
        if(typeNode.prefix) {
          addPrefixMapping(typeNode.prefix, arangoEdges);
        }
        // Type mapped to either <prefix>:<value> or just <value> if no prefix defined
        rootMapping.type.push((typeNode.prefix ? vocabularyMapping[property.prefix].namespace : '') + typeNode.constant);
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
          for(var j = 0; j < graphMapping.graphRoots.length; ++j) {
            comparedRoot = graphMapping.graphRoots[j];
            // We search for URI nodes in the roots that are mapped to the same URI 
            if(isSameUriNode(uriObject, comparedRoot, line) && !comparedRoot.mappingToArango) {
              foundUriNodeNotYetMapped = true;
              break;
            }
          }
          var uriObjectMapping = {};

          // If the URI node has not yet been mapped - map it in order to get the ID to add antry to the Edge collection.
          // The entry connects the currently mapped root to the matched node that was just mapped.
          if (foundUriNodeNotYetMapped) {
            uriObjectMapping = mapURINode(comparedRoot, graphMapping, line, arangoValues, arangoEdges);
          } else {
            // Need to create a new node and a link in the edge collection
            uriObjectMapping = mapURINode(uriObject, graphMapping, line, arangoValues, arangoEdges);
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
          if(property.subElements[0].__type == 'ConstantLiteral') {
            var attributeMappingValue = property.subElements[0].literalValue;
          } else if (property.subElements[0].__type == 'ColumnLiteral') {
            var attributeMappingValue = line[headings[property.subElements[0].literalValue.value]];
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

// Recursive function that maps a line according to graph roots definition in a graph mapping and adds the result 
// to the collections for ArangoDB values and edges
function mapURINode(uriNode, graphMapping, line, arangoValues, arangoEdges) {
  var blankNodeCounter = 0;
  uriNode.mappingToArango = true;
  var uriNodeMapping = {
    type: []
  };

  if (uriNode.__type === 'ColumnURI' || uriNode.__type === 'ConstantURI') {
    // URI root - continue as planned
    if (uriNode.__type === 'ColumnURI') {
      // Get column value from the line
      var columnValue = line[headings[uriNode.column.value]];
      if(!columnValue) {
        if(!valuesNotFoundWarning){
          console.log('WARNING: Some of the column values were not found for the provided mapping!');
          valuesNotFoundWarning = true;
        }
      } else {
        uriNodeMapping.value = columnValue;
        // Add the prefix if it is available and the RDF value
        if(uriNode.prefix) {
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
      if(!constantValue) {
        if(!valuesNotFoundWarning){
          console.log('WARNING: Some of the column values were not found for the provided mapping!');
          valuesNotFoundWarning = true;
        }
      } else {
        uriNodeMapping.value = constantValue;
        // Add the prefix if it is available and the RDF value
        if(uriNode.prefix) {
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
    if(uriNodeMapping.rdf) {
      // Add the mapping of the root to the Arango values if we already mapped this URI node
      if(!mappedUriNodesMap.has(uriNodeMapping.rdf.hashCode().toString())) {
        uriNodeMapping._key = uriNodeMapping.rdf.hashCode().toString();
        arangoValues.push(uriNodeMapping);
        mappedUriNodesMap.set(uriNodeMapping._key, true);
      } 
    } else {
      if(!incompleteMappingWarning) {
        incompleteMappingWarning = true;
        console.log("WARNING: incomplete mapping for object detected!");
      }
    }

    // Go through the properties of the graph root
    for(var i = 0; i < uriNode.subElements.length; ++i) {
      mapProperty(uriNode.subElements[i], uriNodeMapping, graphMapping, line, arangoValues, arangoEdges);
    }

  } else {
    // Literal root node - strange, but OK!

    // give it an ID
    // add to the collection of stuff to add to arango
  }



  // delete the root being mapped from the mapping

  // return the mapping key - may be needed for the edge collection
  return uriNodeMapping;
}

function mapAsyncFlat(graphMapping, line, arangoValues, arangoEdges) {
  // URI nodes are documents
  // All literals are attributes (except label, which is "label")
  // Type also added as "type"
  for (i = 0; i < graphMapping.graphRoots.length; ++i) {
    var graphRoot = graphMapping.graphRoots[i];
    // If we have not yet mapped the root, mark the node and map it
    if(!graphMapping.graphRoots[i].mappingToArango) {
      mapURINode(graphRoot, graphMapping, line, arangoValues, arangoEdges);
    }
  }
  // Reset the graph mapping object
  for (i = 0; i < graphMapping.graphRoots.length; ++i) {
    graphMapping.graphRoots[i].mappingToArango = false;
  }
}

// output file name without the file extension
var stamp = (args.file || args.f).substring((args.file || args.f).lastIndexOf('/') === -1 ? 0 : (args.file || args.f).lastIndexOf('/') + 1, (args.file || args.f).lastIndexOf('.'));

var arangoValuesFilePath = "./results/" + stamp + "_arango_value.json";
var arangoEdgeFilePath = "./results/" + stamp + "_arango_edge.json";

// Open write streams to the files for the value and edge collections
var wsValues = fs.createWriteStream(arangoValuesFilePath);
var wsEdges = fs.createWriteStream(arangoEdgeFilePath);


// empty the output files if they exist
if(fs.existsSync(arangoValuesFilePath)){
  fs.truncateSync(arangoValuesFilePath, 0);
}

if(fs.existsSync(arangoEdgeFilePath)){
  fs.truncateSync(arangoEdgeFilePath, 0);
}

// Initialise files
fs.appendFileSync(arangoValuesFilePath, '');
// Edge collection must be an array so we add the opening bracket before we start adding the individual edges
fs.appendFileSync(arangoEdgeFilePath, '');



function read(input) {
  var createdHeadings = false;
  // reset headings
  headings = []; 

  var lineReader = require('readline').createInterface({
    terminal: false,
    input: fs.createReadStream(input)
  });

  console.log("starting transformation");

  lineReader.on('line', function (line) {
    //console.log(arango_value2);    
    var arango_value = [],
        arango_edge = [];
    var lineBak = line;
    line = pa.parse(line);
    ++lineCounter;
    //    console.log("Line: " + lineCounter);

    if(!createdHeadings) {
      createdHeadings = true;
      for (var i = 0; i < line.data[0].length; i++) { //for all elements i colums array from datalib parse
        var value = line.data[0][i];
        headings[value] = i; //add an entry with value as key and arrayposition as value
      }
    } else {
      //      var rootNode = {"_key":count.toString(), "label": "Start node", "value": "Start node"};
      countToBeRemoved ++;

      if(!line.data[0]){
        // Apparently sometimes there are random empty lines in the input files...
        console.log("Empty or corrupted line found - ignoring ...");
        console.log(line);
        console.log("Line number: " + lineCounter + ". Line text: ");
        console.log(lineBak);
      } else {
        //        arango_edge.push({"_from": 0, "_to":(count).toString()});
        mapAsyncFlat(graph_mapping, line.data[0], arango_value, arango_edge);
        //        arango_value.push(rootNode);
        //        debugger;
        write_object(arango_value, arango_edge);
      }
    }
  });

  lineReader.on('close', function(){
    console.log(arangoPrefixes);

    for (var key in arangoPrefixes) {
      if (arangoPrefixes.hasOwnProperty(key)) {
        fs.appendFileSync(arangoValuesFilePath, JSON.stringify(arangoPrefixes[key]) + '\n');
      }
    }


    console.log("Done!");
    console.timeEnd("transformData");
    console.log("Lines: " + lineCounter);
    // Edge collection must be an array so we add the closing bracket after we are done reading input
    //wsEdges.write(']');

    // Close streams
    wsValues.end();
    wsEdges.end();
    //    fs.appendFileSync("./results/" + stamp + "_arango_edge.json", '{}]');
  });
}

function write_object(arangoValue, arangoEdge){
  // Write nodes
  for(var i = 0; i < arangoValue.length; i++){
    wsValues.write(JSON.stringify(arangoValue[i]) + '\n');
    //    fs.appendFileSync("./results/" + stamp + "_arango_value.json", JSON.stringify(arangoValue[i]) + '\n'); 
  }

  // Write edges
  for(i = 0; i < arangoEdge.length; i++){
    wsEdges.write(JSON.stringify(arangoEdge[i]) + '\n');
    //    fs.appendFileSync("./results/" + stamp + "_arango_edge.json", JSON.stringify(arangoEdge[i]) + ',\n'); 
  }
}

try {
  if(args.transformation || args.t) {
    var transformation = JSON.parse(fs.readFileSync(args.transformation || args.t));
    transformation_vocabs = transformation.extra.rdfVocabs;
    graph_mapping = transformation.extra.graphs[0];
    /* Make vocabs for dataset */
    for(var i = 0; i < transformation_vocabs.length; i++) {
      if(i === 0){ vocabularyMapping = []; }
      var key = transformation_vocabs[i].name;
      vocabularyMapping[key] = transformation_vocabs[i];
    }
  } else {
    throw ("Transformation argument not found.")
  }
} catch(e) {
  console.log("Error reading transformation!");
  console.log(e);
}

try {
  if(args.file || args.f) {
    console.time("transformData");
    read(args.file || args.f);
  } else {
    throw("Input file argument not found!");
  }
} catch (e) {
  console.log("Error reading input file!");
  console.log(e);
}

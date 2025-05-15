
## TREE SCHEMA 

let schema = TreeSchema::new()
            .add_key    <-- ByteBox Type
            .add_value  <-- ByteBox Type
            .build();

let tree = BTreeBuilder::(TreeSchema)
// Stores and Serializes the schema

let entry : TreeSchemaEntry = TreeSchemaEntry {
    key:   ByteBox
    value: ByteBox 
}



PUT  "200" "Andre"  -> tree.insert(Byte_Box::small_int(200) , Byte_Box::varchar("Andre"))
GET  "200"          -> tree.get_entry()

SCAN ".."                     -> Full Scan
SCAN "..end_bound"            -> Up to a certain range exclusive
SCAN "..=end_bound"           -> Up to a certain range inclusive  
SCAN "start_bound.."          -> Full Scan exclusive of a start bound
SCAN "start_bound=.."          -> Full Scan inclusive of a start bound
SCAN "start_bound..end_bound" -> Full Scan between bounds


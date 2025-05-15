## Tree Serialization


pub enum NodeType {

    Internal(Vec< PageId >, Vec < Key< ByteBox > >, PageId),

    Leaf(Vec< TreeSchemaEntry >, NextPointer, PageId),

    Unexpected,
}

RAW PAGE = [
    INTERNAL 
     ____________________________________________________________________________________
    |_____________________________________HEADER_________________________________________|
    |_______________________________Node Specific Data___________________________________|
    | [ Pointer | Key_Len | Key  ] [  Pointer | Key_Len | Key  ] [  Pointer |Key_Len | ] | 
    | [ Pointer | Key_Len | Key  ] [  Pointer | Key_Len | Key  ] [  Pointer |Key_Len | ] | 
    | [ Pointer | Key_Len | Key  ] [  Pointer | Key_Len | Key  ] [  Pointer |Key_Len | ] | 
    | [ Pointer | Key_Len | Key  ] [  Pointer | Key_Len | Key  ] [  Pointer |Key_Len | ] | 
    | [ Pointer | Key_Len | Key  ] [  Pointer | Key_Len | Key  ] [  Pointer |Key_Len | ] | 
    | [ Pointer | Key_Len | Key  ] [  Pointer | Key_Len | Key  ] [  Pointer |Key_Len | ] | 
    | [ Pointer | Key_Len | Key  ] [  Pointer | Key_Len | Key  ] [  Pointer |Key_Len | ] | 
    | [ Pointer | Key_Len | Key  ] [  Pointer | Key_Len | Key  ] [  Pointer |Key_Len | ] | 
    | [ Pointer | Key_Len | Key  ] [  Pointer | Key_Len | Key  ] [  Pointer |Key_Len | ] | 
    | [ Pointer | Key_Len | Key  ] [  Pointer | Key_Len | Key  ] [  Pointer |Key_Len | ] | 
    |____________________________________________________________________________________|


    ]


RAW PAGE = [
    Leaf 
     ___________________________________________________________________________
    |___________________________________HEADER__________________________________|
    |___________________________ _Node Specific Data____________________________|
    | [ Key_Len | Key | Data_len | Data  ] [ Key_Len | Key | Data_len | Data  ] | 
    | [ Key_Len | Key | Data_len | Data  ] [ Key_Len | Key | Data_len | Data  ] |
    | [ Key_Len | Key | Data_len | Data  ] [ Key_Len | Key | Data_len | Data  ] | 
    | [ Key_Len | Key | Data_len | Data  ] [ Key_Len | Key | Data_len | Data  ] |  
    | [ Key_Len | Key | Data_len | Data  ] [ Key_Len | Key | Data_len | Data  ] | 
    | [ Key_Len | Key | Data_len | Data  ] [ Key_Len | Key | Data_len | Data  ] | 
    |___________________________________________________________________________|

]


Bare in mind that the Key and Data Lengths are both based on the lengths of the Bytebox datatype they were built with.
Varlen entries aren't neccessarily of the same length but same fixed upper bound length.The table schema object ensures that all data inserted into the KV store are valid

decode( RAW PAGE ) -> NodeType
    node_type = get_type(RAW PAGE)
    data = RAW PAGE[COMMON_NODE_HEADER_SIZE..]
    get_pointer - get_pointer( RAW PAGE )

    match node_type {
        Internal => {
            let node = Decode::Internal( using table_codec; data[offset..])?
            node.pointer = pointer

        }

        Leaf => {
            let node = Decode::Internal( using table_codec; data[offset..])?
            node.pointer = pointer

        }

}

Encode( Node ) -> RAW PAGE
    // Builds the table page one entry at a time 
    match node_type {
        Internal => {
            let node = Encode::Internal( using table_codec; Node)? 
            node.pointer = pointer

        }

        Leaf => {
            let node = Encode::Internal( using table_codec; Node)?
            node.pointer = pointer

        }

}





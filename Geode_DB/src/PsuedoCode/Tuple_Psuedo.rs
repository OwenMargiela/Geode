/*
    schema == Schema::Build
    schema.Int( "Salary") name
    schema.addChar("Name", 15)  name + size
    schema.addVarchar("Address", 50) name + size
    schema.addVarchar("Mom's_Address", 50) name + size


    Struct that holds type formation
    ByteBox
        data
        data_type
        data_size
        data_length : specificly here to separate var length fields

    to_byte_box()
        Serializes and provides meta data on its type

    Previous implementations of data types grew in complexity due to my tendacies to program
    in and object oriented manner.


    tuple::build( values: Vec<Bytebox> , schema: Schema ) -> tuple
    tuple::build_with_bytestream( stream: Vec<u8> , schema: Schema ) -> tuple
    tupe::copy( tuple, schema: Schema)

    A tuple bytestream of data representing a database row



    Also a null bitmap would be usefull
    | Header |len| |header_len| version| |var_ptr| |var_ptr| | 0  | Body |fixed_length_field| ||len|var_field| ||len|var_field|   |
                                            |          |_________________________________________|                |
                                            |_____________________________________________________________________|

    Slotted Page Structure

    |len| body | |len|  body | |len| body | |len| body |

    |len| body | |len|  body | |len| body | |len| body |

    |len| body | |len|  body | |len| body | |len| body |

    |len| body | |len|  body | |len| body | |len| body |

    ^
    |_______________________________
                                    |
    |Foot |0|1|2|3|4|5|6|7|7|7| free_space pointer       |

    Also meta data including
        version
        free space size



    A special page known as the page directory will be implemented to facilitate
    the retreival of meta data for a specific page. Location on disk
    (filename or pointer to the beginning offset of a file ), the amount of freespace
    and a list of empty pages.


    Page directory
    Entry
    | Page_ID | f_ptr | amt_frspc | | pg_type | empty_bool |

    I was thinking of a page that would be a global free list that is entirely
    filled with free_space_pointer offsets. Logistically it might be alot to manage

    If a page only has one free slot does it make sense to bring that one page into memory
    just to insert one tuple?

    | Page_ID | frspc_ptr | frspc_size | |

    Denormalized Tuple Data: If two tables are related, the DBMS can “pre-join” them, so the tables end up
    on the same page. This makes reads faster since the DBMS only has to load in one page rather than two
    separate pages. However, it makes updates more expensive since the DBMS needs more space for each
    tuple.


    Implement reordering and padding for word alignment


*/

/*

    Directly casting types to variables

    big_int = BigInt::wrap(250_000);
    int     = Int::wrap(25_000);
    small_int = SmallInt::wrap(1_000);
    decimal = Decimal::wrap(55_000.00);

    bool    = Boolean::wrap(true);

    char    = Char::wrap("Hello World!", 15);
    varchar = Varchar::wrap("Hello World", 15);

    Directly serializes a value to the bytebox of a specific type

    big_int =  ByteBox::BigInt(250_000);
            Results in the struct {
                data : Vec<u8>
                data_type : BIGINT
                data_size : size of i64
                data_length : same as size for all types except varchar
            }

    int     =  ByteBox::Int(25_000);
    small_int =  ByteBox::SmallInt(1_000);
    decimal =  ByteBox::Decimal(55_000.00);

    bool    =  ByteBox::Boolean(true);

    char    =  ByteBox::Char("Hello World!", 15);

    varchar_box =  ByteBox::Varchar("Hello World", 15);

            Results in the struct {
                data : Vec<u8>
                data_type : Varchar
                data_size : 15 maximum character length
                data_length : 10 used character length
            }


    The return value of the to_byte_box method would yeild the same struct
    varchar_box == varchar_to_byte_box()


    tuple::build( values: Vec<Bytebox> , schema: Schema ) -> tuple

*/

/*
 imple tuple

    tuple::build( values: Vec<Bytebox> , schema: Schema ) -> Option<tuple>

        iterate through the vector and the schema to checks its validity ( nullable schemas will be delt with later )
            bytebox.gettype() is should be implemented

        Gather data necessary to construct tuple header ( length,
                                                        header length,
                                                        version,
                                                        number of variable length fields,
                                                        pointers to those fields,
                                                        data body )


        A null byte could be used as an escape character that separates the header from the body
        The thing is we want to know for a fact that this terminator doesnt conflict with any character
        that a db user might want to be placed in the database


        fields in the header are of fixed lengths. If it says we have only have 2 car_length fields, just do two iterations


        deserialize ( schema ) -> Result< Vec<u8>, error >
            destructures the length of the tuple and header
            the version number
            uses the schema to aid in the extraction of fixed length fields
            iterates number of var_length times, jumping to pointer offsets and deserializes var length fields

        reconstruct_bytebox_vector( schema ) -> Result< Vec<bytebox>, error >

        == operator()  -> bool
            Bytebox by Bytebox equality operator

        getColumn ( schema, name ) Result< bytebox, error >

        getColumn_bytestream ( schema, name )-> Result< Vec<u8> , error >

        deafault () -> tuple
            Default dummy tuple

*/

/*

    big_int =  ByteBox::BigInt(250_000);
    int     =  ByteBox::Int(25_000);
    small_int =  ByteBox::SmallInt(1_000);
    decimal =  ByteBox::Decimal(55_000.00);

    bool    =  ByteBox::Boolean(true);

    char    =  ByteBox::Char("Hello World!", 15);

    varchar =  ByteBox::Varchar("Hello World", 15);

    let bytebox_vec = Vec::<ByteBox>::with_capacity(7);

    bytebox_vec.extend( big_int, int, small_int, decimal, bool, char, varchar );


    let schema = SchemaBuilder::new()
        .add_big_int("col_1")
        .add_int("col_2")
        .add_small_int("col_3")
        .add_decimal("col_4")
        .add_decimal("col_5")
        .add_bool("col_6")
        .add_char("col_7")
        .add_varchar("col_8")
        .build();

    let tuple = tuple::build( bytebox_vec, schema )

    let tuple_two = tuple::build( bytebox_vec, schema )

    assert_eq( tuple, tuple_two )


    let reconstructed_bytebox = reconstruct_bytebox_vector();
    for i in range 0..bytebox.len {

        assert_eq( bytebox[i], reconstructed_bytebox[i] );

    }

    let box = tuple.getColumn( schema, "col_5");
    let byte_stream = tuple.getColumn_bytestream( schema, "col_5" );

    for i in range 0..box.len {

       assert_eq( box.data[i], byte_stream[i] )

    }





*/

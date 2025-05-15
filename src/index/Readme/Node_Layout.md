
## Tree Layout


/// Common Node header layout (Ten bytes in total)
    IS_ROOT_SIZE: usize = 1;
    IS_ROOT_OFFSET: usize = 0;
    NODE_TYPE_SIZE: usize = 1;
    NODE_TYPE_OFFSET: usize = 1;
    CURRENT_PAGE_POINTER_OFFSET: usize = 2; 
    CURRENT_PAGE_POINTER_SIZE: usize = PTR_SIZE; // 8 
    COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + POINTER_SIZE;


/// Leaf node header layout (Eighteen bytes in total)

    Space for keys and values: PAGE_SIZE - LEAF_NODE_HEADER_SIZE = 4096 - 26 = 4070 bytes.
    Which leaves 4076 / keys_limit = 20 (ten for key and 10 for value).
    
    LEAF_NODE_NUM_PAIRS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
    LEAF_NODE_NUM_PAIRS_SIZE: usize = PTR_SIZE;

    NEXT_LEAF_POINTER_OFFSET: usize =  COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_PAIRS_SIZE ;
    NEXT_LEAF_POINTER_SIZE : usize = PTR_SIZE;

    LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_PAIRS_SIZE + NEXT_LEAF_POINTER_SIZE;
             26                  =           10            +           8              +           8


    DATA LAYOUT
    | KEY_LENGTH | KEY | DATA_LEN | DATA |

/// Internal header layout (Eighteen bytes in total)

    Space for children and keys: PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE = 4096 - 18 = 4078 bytes.
    INTERNAL_NODE_NUM_CHILDREN_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
    INTERNAL_NODE_NUM_CHILDREN_SIZE: usize = PTR_SIZE;
    INTERNAL_NODE_HEADER_SIZE: usize =  18  = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_CHILDREN_SIZE;
                                                           10         +           8

    DATA LAYOUT
    | KEY_LENGTH | KEY | 



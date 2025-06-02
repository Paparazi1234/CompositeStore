# define your pluggable implementations building library names in this file

# define multi_versions implementations building library names here
set(SEQ_BASED_MULTI_VERSIONS_STATIC_LIB seq_based_multi_versions)
set(SEQ_BASED_MULTI_VERSIONS_SHARED_LIB seq_based_multi_versions-shared)

set(MULTI_VERSIONS_STATIC_LIB_LINKING_IMPLS
        ${SEQ_BASED_MULTI_VERSIONS_STATIC_LIB})

set(MULTI_VERSIONS_SHARED_LIB_LINKING_IMPLS
        ${SEQ_BASED_MULTI_VERSIONS_SHARED_LIB})

# define store implementation building library names here
set(SKIPLIST_BACKED_STORE_STATIC_LIB skiplist_backed_store)
set(SKIPLIST_BACKED_STORE_SHARED_LIB skiplist_backed_store-shared)

set(STORE_STATIC_LIB_LINKING_IMPLS
        ${SKIPLIST_BACKED_STORE_STATIC_LIB})

set(STORE_SHARED_LIB_LINKING_IMPLS
        ${SKIPLIST_BACKED_STORE_SHARED_LIB})


# the library that links all multi_versions implementations defined above, don't touch
set(MULTI_VERSIONS_STATIC_LIB multi_versions)
set(MULTI_VERSIONS_SHARED_LIB multi_versions-shared)

# the library that links all store implementations defined above, don't touch
set(STORE_STATIC_LIB store)
set(STORE_SHARED_LIB store-shared)

# txn lock manager library names
set(TXN_LOCK_MANAGER_STATIC_LIB txn_lock_manager)
set(TXN_LOCK_MANAGER_SHARED_LIB txn_lock_manager-shared)
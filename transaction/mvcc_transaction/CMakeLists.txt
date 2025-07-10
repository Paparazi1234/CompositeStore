set(MVCC_TRANSACTION_SOURCES
        mvcc_transaction.cc
        optimistic_transaction/optimistic_transaction.cc
        pessimistic_transaction/pessimistic_transaction.cc
        pessimistic_transaction/write_committed_transaction.cc
        pessimistic_transaction/write_prepared_transaction.cc)

add_library(${MVCC_TRANSACTION_SHARED_LIB} SHARED ${MVCC_TRANSACTION_SOURCES})
target_link_libraries(${MVCC_TRANSACTION_SHARED_LIB} ${MVCC_TXN_STORE_SHARED_LIB})

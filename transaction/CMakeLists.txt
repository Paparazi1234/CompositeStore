set(TRANSACTION_SOURCES
        mvcc_transaction/optimistic_transaction/optimistic_transaction.cc
        mvcc_transaction/pessimistic_transaction/pessimistic_transaction.cc
        mvcc_transaction/pessimistic_transaction/write_committed_transaction.cc
        mvcc_transaction/pessimistic_transaction/write_prepared_transaction.cc
        mvcc_transaction/mvcc_transaction.cc
        transaction_impl.cc)

add_library(${TRANSACTION_SHARED_LIB} SHARED ${TRANSACTION_SOURCES})
target_link_libraries(${TRANSACTION_SHARED_LIB} ${MVCC_TXN_STORE_SHARED_LIB})

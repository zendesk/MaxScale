configure_file(test_ssl.sh ${CMAKE_CURRENT_BINARY_DIR}/test_ssl.sh @ONLY)
configure_file(no_ca.cnf ${CMAKE_CURRENT_BINARY_DIR}/no_ca.cnf @ONLY)
configure_file(no_server_cert.cnf ${CMAKE_CURRENT_BINARY_DIR}/no_server_cert.cnf @ONLY)
configure_file(no_server_key.cnf ${CMAKE_CURRENT_BINARY_DIR}/no_server_key.cnf @ONLY)
configure_file(bad_ca.cnf ${CMAKE_CURRENT_BINARY_DIR}/bad_ca.cnf @ONLY)
configure_file(bad_cert.cnf ${CMAKE_CURRENT_BINARY_DIR}/bad_cert.cnf @ONLY)
configure_file(bad_key.cnf ${CMAKE_CURRENT_BINARY_DIR}/bad_key.cnf @ONLY)
configure_file(bad_ssl.cnf ${CMAKE_CURRENT_BINARY_DIR}/bad_ssl.cnf @ONLY)
configure_file(bad_ssl_version.cnf ${CMAKE_CURRENT_BINARY_DIR}/bad_ssl_version.cnf @ONLY)
configure_file(ok.cnf ${CMAKE_CURRENT_BINARY_DIR}/ok.cnf @ONLY)
add_test(NAME SSLTest COMMAND ${CMAKE_CURRENT_BINARY_DIR}/test_ssl.sh)

# Apache 2 Licensed
# Code modified from:
# TimescaleDB (https://www.timescale.com/)

set(EXT_SQL_FILE ${CMAKE_CURRENT_BINARY_DIR}/${EXT_NAME}--${PROJECT_VERSION}.sql)
#set(EXT_SQL_UPDATE_FILE ${PROJECT_NAME}--${UPDATE_FROM_VERSION}--${PROEJCT_VERSION}.sql)
#set(EXT_SQL_UPDATE_PRE_FILE updates/pre-${UPDATE_FROM_VERSION}--${PROEJCT_VERSION}.sql)
#set(EXT_SQL_UPDATE_POST_FILE updates/post-${UPDATE_FROM_VERSION}--${PROEJCT_VERSION}.sql)

set(INSTALL_SQL_FILES
#        simple.sql
        install/00-type.sql
        install/01-indexing.sql
        install/02-inspection.sql
        install/03-traversal.sql
        install/04-hierarchy.sql
        install/05-regions.sql
        install/06-uniedge.sql
        install/07-miscellaneous.sql
        install/10-extension.sql
        install/11-opclass_btree.sql
        install/12-opclass_hash.sql
        install/99-postgis.sql
)

foreach(file ${INSTALL_SQL_FILES})
        file(READ ${file} CONTENTS)
        file(APPEND ${EXT_SQL_FILE} "${CONTENTS}")
endforeach()

install(
        FILES ${EXT_SQL_FILE}
        DESTINATION "${PostgreSQL_SHARE_DIR}/extension"
        COMPONENT extension
)

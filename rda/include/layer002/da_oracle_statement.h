#ifndef DA_ORACLE_STATEMENT_H
#define DA_ORACLE_STATEMENT_H

#include "layer000/da_types.h"

RT_EXPORT rt_s da_oracle_statement_execute(struct da_statement *statement, const rt_char8 *sql);

RT_EXPORT rt_s da_oracle_statement_get_row_count(struct da_statement *statement, rt_un *row_count);

RT_EXPORT rt_s da_oracle_statement_free(struct da_statement *statement);

#endif /* DA_ORACLE_STATEMENT_H */


import string
from operator import itemgetter
from collections import namedtuple
from copy import deepcopy
import itertools

_ALL_DATABASES = {}


class TableExistsError(Exception): pass
class AlreadyInTransactionError(Exception): pass
class NotInTransactionError(Exception): pass
class LockInterferenceError(Exception): pass


WhereClause = namedtuple("WhereClause", ["col_name", "operator", "constant"])
UpdateClause = namedtuple("UpdateClause", ["col_name", "constant"])
FromJoinClause = namedtuple("FromJoinClause", ["left_table_name",
                                               "right_table_name",
                                               "left_join_col_name",
                                               "right_join_col_name"])


connection_id = 0

class Connection(object):

    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database


        global connection_id
        self.in_transaction = False
        self.unique_id = connection_id
        connection_id = connection_id + 1
        
        self.database.connections.append(self.unique_id)
        
        self.local_database = None
        
        
        
        
    def create_collation(self, name, function):
        self.database.collations[name] = function
        return
        
        
    def executemany(self, statement, list_of_values):
        value_indexes = []
        for i in range(len(statement)):
            if statement[i] == '?':
                value_indexes.append(i)
            else:
                continue

        for i in list_of_values:
            intended_values = list(zip(i, value_indexes))
            execute_statement = list(statement)

            for j in range(len(intended_values)):
                execute_statement[intended_values[j][1]] = intended_values[j][0]
                
            execute_statement = [str(k) for k in execute_statement]
            self.execute(''.join(execute_statement))

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        
        def create_view(tokens):
            # if self.in_transaction == True:
            # else:
            
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "VIEW")
            view_name = tokens.pop(0)
            pop_and_check(tokens, "AS")
            view_table = tokens[tokens.index("FROM") + 1]
                
            view_statement = select(tokens, True)

            self.database.views[view_name] = (view_statement, view_table)
        
        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            
            if tokens[1] == "VIEW":
                create_view(tokens)
                return 
            
            
            if "exclusive" in self.database.locks:
                if self.database.locks["exclusive"] != self.unique_id:
                    raise LockInterferenceError("create 1 cant do that cuz of ze locks")
            if "reserved" in self.database.locks:
                if self.database.locks["reserved"] != self.unique_id:
                    raise LockInterferenceError("create 2 cant do that cuz of ze locks")
            
            if self.in_transaction == True:
                
                self.database.locks["exclusive"] = self.unique_id
                    
                pop_and_check(tokens, "CREATE")
                pop_and_check(tokens, "TABLE")
                
                if tokens[0] == "IF" and tokens[1] == "NOT":
                    pop_and_check(tokens, "IF")
                    pop_and_check(tokens, "NOT")
                    pop_and_check(tokens, "EXISTS")
                    table_name = tokens.pop(0)
                    if table_name in self.local_database.tables:
                        return
                    
                else:
                    table_name = tokens.pop(0)
                    if table_name in self.local_database.tables:
                        raise TableExistsError("Table already exists")
                    
                    
                pop_and_check(tokens, "(")
                column_name_type_pairs = []
                while True:
                    column_name = tokens.pop(0)
                    qual_col_name = QualifiedColumnName(column_name, table_name)
                    column_type = tokens.pop(0)
                    assert column_type in {"TEXT", "INTEGER", "REAL"}
                    
                    default_value = ''
                    if tokens[0] == "DEFAULT":
                        pop_and_check(tokens, "DEFAULT")
                        default_value = tokens.pop(0)
                        
                    column_name_type_pairs.append((qual_col_name, (column_type, default_value)))
                    
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','
                self.local_database.create_new_table(table_name, column_name_type_pairs)
                
            else:
            
                pop_and_check(tokens, "CREATE")
                pop_and_check(tokens, "TABLE")
                
                
                if tokens[0] == "IF" and tokens[1] == "NOT":
                    pop_and_check(tokens, "IF")
                    pop_and_check(tokens, "NOT")
                    pop_and_check(tokens, "EXISTS")
                    table_name = tokens.pop(0)
                    if table_name in self.database.tables:
                        return
                    
                else:
                    table_name = tokens.pop(0)
                    if table_name in self.database.tables:
                        raise TableExistsError("Table already exists")
                    
                pop_and_check(tokens, "(")
                column_name_type_pairs = []
                while True:
                    column_name = tokens.pop(0)
                    qual_col_name = QualifiedColumnName(column_name, table_name)
                    column_type = tokens.pop(0)
                    assert column_type in {"TEXT", "INTEGER", "REAL"}
                    
                    default_value = ''
                    if tokens[0] == "DEFAULT":
                        pop_and_check(tokens, "DEFAULT")
                        default_value = tokens.pop(0)
                        
                    column_name_type_pairs.append((qual_col_name, (column_type, default_value)))
                        
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','
                self.database.create_new_table(table_name, column_name_type_pairs)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            
            if "exclusive" in self.database.locks:
                if self.database.locks["exclusive"] != self.unique_id:
                    raise LockInterferenceError("insert 1 cant do that cuz of ze locks")
            if "reserved" in self.database.locks:
                if self.database.locks["reserved"] != self.unique_id:
                    raise LockInterferenceError("insert 2 cant do that cuz of ze locks")
                    
                    
            if self.in_transaction == True:
                if "exclusive" not in self.database.locks:
                    self.database.locks["reserved"] = self.unique_id
                def get_comma_seperated_contents(tokens):
                    contents = []
                    pop_and_check(tokens, "(")
                    while True:
                        item = tokens.pop(0)
                        contents.append(item)
                        comma_or_close = tokens.pop(0)
                        if comma_or_close == ")":
                            return contents
                        assert comma_or_close == ',', comma_or_close
    
                pop_and_check(tokens, "INSERT")
                pop_and_check(tokens, "INTO")
                table_name = tokens.pop(0)
                if tokens[0] == "(":
                    col_names = get_comma_seperated_contents(tokens)
                    qual_col_names = [QualifiedColumnName(col_name, table_name)
                                      for col_name in col_names]
                else:
                    qual_col_names = None
                    
                    
                if tokens[0] == "DEFAULT":
                    pop_and_check(tokens, "DEFAULT")
                    pop_and_check(tokens, "VALUES")
                    self.local_database.insert_into(table_name,
                                                  [],
                                                  qual_col_names=qual_col_names)
                else:
                    pop_and_check(tokens, "VALUES")
                    while tokens:
                        row_contents = get_comma_seperated_contents(tokens)
                        if qual_col_names:
                            assert len(row_contents) == len(qual_col_names)
                        self.local_database.insert_into(table_name,
                                                  row_contents,
                                                  qual_col_names=qual_col_names)
                        if tokens:
                            pop_and_check(tokens, ",")
                        
            else:
                
                def get_comma_seperated_contents(tokens):
                    contents = []
                    pop_and_check(tokens, "(")
                    while True:
                        item = tokens.pop(0)
                        contents.append(item)
                        comma_or_close = tokens.pop(0)
                        if comma_or_close == ")":
                            return contents
                        assert comma_or_close == ',', comma_or_close
    
                pop_and_check(tokens, "INSERT")
                pop_and_check(tokens, "INTO")
                table_name = tokens.pop(0)
                if tokens[0] == "(":
                    col_names = get_comma_seperated_contents(tokens)
                    qual_col_names = [QualifiedColumnName(col_name, table_name)
                                      for col_name in col_names]
                else:
                    qual_col_names = None
                    
                if tokens[0] == "DEFAULT":
                    pop_and_check(tokens, "DEFAULT")
                    pop_and_check(tokens, "VALUES")
                    self.database.insert_into(table_name,
                                                  [],
                                                  qual_col_names=qual_col_names)
                else:
                    pop_and_check(tokens, "VALUES")
                    while tokens:
                        row_contents = get_comma_seperated_contents(tokens)
                        if qual_col_names:
                            assert len(row_contents) == len(qual_col_names)
                        self.database.insert_into(table_name,
                                                  row_contents,
                                                  qual_col_names=qual_col_names)
                        if tokens:
                            pop_and_check(tokens, ",")




        def aggregate_get_qualified_column_name(tokens):
            print("TOKENS TOKENS TOKENS :  ", tokens)
            token.pop(0)
            token.pop(0)
            token.pop(0)
            possible_col_name = tokens.pop(0)
            if tokens and tokens[0] == '.':
                tokens.pop(0)
                actual_col_name = tokens.pop(0)
                table_name = possible_col_name
                return QualifiedColumnName(actual_col_name, table_name)
            return QualifiedColumnName(possible_col_name)
            
            
        def get_qualified_column_name(tokens):
            """
            Returns comsumes tokens to  generate tuples to create
            a QualifiedColumnName.
            """
            possible_col_name = tokens.pop(0)
            if tokens and tokens[0] == '.':
                tokens.pop(0)
                actual_col_name = tokens.pop(0)
                table_name = possible_col_name
                return QualifiedColumnName(actual_col_name, table_name)
            return QualifiedColumnName(possible_col_name)

        def update(tokens):
            
            
            if "exclusive" in self.database.locks:
                if self.database.locks["exclusive"] != self.unique_id:
                    raise LockInterferenceError("update 1 cant do that cuz of ze locks")
            if "reserved" in self.database.locks:
                if self.database.locks["reserved"] != self.unique_id:
                    raise LockInterferenceError("update 2 cant do that cuz of ze locks")
                    
                    
            
            if self.in_transaction == True:
                self.database.locks["exclusive"] = self.unique_id
                pop_and_check(tokens, "UPDATE")
                table_name = tokens.pop(0)
                pop_and_check(tokens, "SET")
                update_clauses = []
                while tokens:
                    qual_name = get_qualified_column_name(tokens)
                    if not qual_name.table_name:
                        qual_name.table_name = table_name
                    pop_and_check(tokens, '=')
                    constant = tokens.pop(0)
                    update_clause = UpdateClause(qual_name, constant)
                    update_clauses.append(update_clause)
                    if tokens:
                        if tokens[0] == ',':
                            tokens.pop(0)
                            continue
                        elif tokens[0] == "WHERE":
                            break
    
                where_clause = get_where_clause(tokens, table_name)
    
                self.local_database.update(table_name, update_clauses, where_clause)
                
            else:
                
                pop_and_check(tokens, "UPDATE")
                table_name = tokens.pop(0)
                pop_and_check(tokens, "SET")
                update_clauses = []
                while tokens:
                    qual_name = get_qualified_column_name(tokens)
                    if not qual_name.table_name:
                        qual_name.table_name = table_name
                    pop_and_check(tokens, '=')
                    constant = tokens.pop(0)
                    update_clause = UpdateClause(qual_name, constant)
                    update_clauses.append(update_clause)
                    if tokens:
                        if tokens[0] == ',':
                            tokens.pop(0)
                            continue
                        elif tokens[0] == "WHERE":
                            break
    
                where_clause = get_where_clause(tokens, table_name)
    
                self.database.update(table_name, update_clauses, where_clause)



        def delete(tokens):
            
            if "exclusive" in self.database.locks:
                if self.database.locks["exclusive"] != self.unique_id:
                    raise LockInterferenceError("delete 1 cant do that cuz of ze locks")
            if "reserved" in self.database.locks:
                if self.database.locks["reserved"] != self.unique_id:
                    raise LockInterferenceError("delete 2 cant do that cuz of ze locks")
            
            if self.in_transaction == True:
                
                self.database.locks["exclusive"] = self.unique_id
                
                pop_and_check(tokens, "DELETE")
                pop_and_check(tokens, "FROM")
                table_name = tokens.pop(0)
                where_clause = get_where_clause(tokens, table_name)
                self.database.delete(table_name, where_clause)
                
            else:
                
                pop_and_check(tokens, "DELETE")
                pop_and_check(tokens, "FROM")
                table_name = tokens.pop(0)
                where_clause = get_where_clause(tokens, table_name)
                self.database.delete(table_name, where_clause)



        def get_where_clause(tokens, table_name):
            if not tokens or tokens[0] != "WHERE":
                return None
            tokens.pop(0)
            qual_col_name = get_qualified_column_name(tokens)
            if not qual_col_name.table_name:
                qual_col_name.table_name = table_name
            operators = {">", "<", "=", "!=", "IS"}
            found_operator = tokens.pop(0)
            assert found_operator in operators
            if tokens[0] == "NOT":
                tokens.pop(0)
                found_operator += " NOT"
            constant = tokens.pop(0)
            if constant is None:
                assert found_operator in {"IS", "IS NOT"}
            if found_operator in {"IS", "IS NOT"}:
                assert constant is None
            return WhereClause(qual_col_name, found_operator, constant)



        def select(tokens, for_view=False, _max = False, _min = False):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            
            if "exclusive" in self.database.locks:
                if self.database.locks["exclusive"] != self.unique_id:
                    raise LockInterferenceError("select 1  cant do that cuz of ze locks")
            
            
            if self.in_transaction == True:
                if "exclusive" not in self.database.locks:
                    if "reserved" in self.database.locks:
                        if self.database.locks["reserved"] != self.unique_id:
                            self.database.locks["shared"] = self.unique_id
                            
                            
            

                def get_from_join_clause(tokens):
                    left_table_name = tokens.pop(0)
                    if tokens[0] != "LEFT":
                        return FromJoinClause(left_table_name, None, None, None)
                    pop_and_check(tokens, "LEFT")
                    pop_and_check(tokens, "OUTER")
                    pop_and_check(tokens, "JOIN")
                    right_table_name = tokens.pop(0)
                    pop_and_check(tokens, "ON")
                    left_col_name = get_qualified_column_name(tokens)
                    pop_and_check(tokens, "=")
                    right_col_name = get_qualified_column_name(tokens)
                    return FromJoinClause(left_table_name,
                                          right_table_name,
                                          left_col_name,
                                          right_col_name)
    
                pop_and_check(tokens, "SELECT")
    
                is_distinct = tokens[0] == "DISTINCT"
                if is_distinct:
                    tokens.pop(0)
    
                output_columns = []
                while True:
                    _min = False
                    _max = False
                    
                    if list(tokens[0])[3] == '(': # min or max
                        if list(tokens[0])[2] == 'x': # max
                            _max = True
                            qual_col_name = get_qualified_column_name(tokens)
                        else: # min
                            _min = True
                            qual_col_name = aggregate_get_qualified_column_name(tokens)
                    else:
                        qual_col_name = get_qualified_column_name(tokens)
                        
                        
                        
                        
                    output_columns.append(qual_col_name)
                    comma_or_from = tokens.pop(0)
                    if comma_or_from == "FROM":
                        break
                    assert comma_or_from == ','
    
                # FROM or JOIN
                from_join_clause = get_from_join_clause(tokens)
                table_name = from_join_clause.left_table_name
    
                # WHERE
                where_clause = get_where_clause(tokens, table_name)
    
                # ORDER BY
                pop_and_check(tokens, "ORDER")
                pop_and_check(tokens, "BY")
                if "DESC" in tokens:
                    order_by_columns = []
                    while True:
                        desc = ''
                        if (len(tokens) > 1 and tokens[1] == "DESC") or \
                            (len(tokens) > 1 and tokens[1] == '.' and tokens[3] == "DESC"):
                            desc = 'True'
                            
                        qual_col_name = get_qualified_column_name(tokens)
                        order_by_columns.append((qual_col_name, desc))
                        
                        if tokens[0] == "DESC":
                            pop_and_check(tokens, "DESC")
                            
                        if not tokens:
                            break
                        pop_and_check(tokens, ",")
                    
                    if for_view == True:
                        
                        return [output_columns,
                        order_by_columns,
                        from_join_clause,
                        where_clause,
                        is_distinct]
                        
                    return self.local_database.select(
                        output_columns,
                        order_by_columns,
                        from_join_clause=from_join_clause,
                        where_clause=where_clause,
                        is_distinct=is_distinct)
                        
                else:
                    order_by_columns = []
                    while True:
                        qual_col_name = get_qualified_column_name(tokens)
                        order_by_columns.append((qual_col_name, ''))
                        if not tokens:
                            break
                        pop_and_check(tokens, ",")
                    if for_view == True:
                        
                        return [output_columns,
                        order_by_columns,
                        from_join_clause,
                        where_clause,
                        is_distinct]
                    return self.local_database.select(
                        output_columns,
                        order_by_columns,
                        from_join_clause=from_join_clause,
                        where_clause=where_clause,
                        is_distinct=is_distinct)
                    
                    
            else:
            
            
                def get_from_join_clause(tokens):
                    left_table_name = tokens.pop(0)
                    if tokens[0] != "LEFT":
                        return FromJoinClause(left_table_name, None, None, None)
                    pop_and_check(tokens, "LEFT")
                    pop_and_check(tokens, "OUTER")
                    pop_and_check(tokens, "JOIN")
                    right_table_name = tokens.pop(0)
                    pop_and_check(tokens, "ON")
                    left_col_name = get_qualified_column_name(tokens)
                    pop_and_check(tokens, "=")
                    right_col_name = get_qualified_column_name(tokens)
                    return FromJoinClause(left_table_name,
                                          right_table_name,
                                          left_col_name,
                                          right_col_name)
    
                pop_and_check(tokens, "SELECT")
    
                is_distinct = tokens[0] == "DISTINCT"
                if is_distinct:
                    tokens.pop(0)
    
                output_columns = []
                aggs = []
                while True:

                    if tokens[1] == '(': # min or max
                        if tokens[0] == 'max': # max
                            tokens.pop(0)
                            tokens.pop(0)
                            tokens.pop(1)
                            qual_col_name = get_qualified_column_name(tokens)
                            aggs.append((qual_col_name, "_max"))
                        else: # min
                            tokens.pop(0)
                            tokens.pop(0)
                            tokens.pop(1)
                            qual_col_name = get_qualified_column_name(tokens)
                            aggs.append((qual_col_name, "_min"))
                    else:
                        qual_col_name = get_qualified_column_name(tokens)
                    

                    output_columns.append(qual_col_name)
                    comma_or_from = tokens.pop(0)
                    if comma_or_from == "FROM":
                        break
                    assert comma_or_from == ','
                # FROM or JOIN
                from_join_clause = get_from_join_clause(tokens)
                table_name = from_join_clause.left_table_name
    
                # WHERE
                where_clause = get_where_clause(tokens, table_name)
    
                # ORDER BY
                pop_and_check(tokens, "ORDER")
                pop_and_check(tokens, "BY")
                if "DESC" in tokens:
                    # do other shit
                    order_by_columns = []
                    while True:
                        collate_function = ''
                        desc = ''
                        if (len(tokens) > 1 and tokens[1] == "DESC") or \
                            (len(tokens) > 1 and tokens[1] == '.' and tokens[3] == "DESC") or \
                            (len(tokens) > 3 and tokens[1] == "COLLATE" and tokens[3] == "DESC"):
                            desc = 'True'
                            
                        qual_col_name = get_qualified_column_name(tokens)
                        
                        if tokens:
                            if tokens[0] == "COLLATE":
                                tokens.pop(0)
                                collate_function = tokens.pop(0)
                            
                        if collate_function != '':
                            order_by_columns.append(((qual_col_name, self.database.collations[collate_function]), desc))
                        else:
                            order_by_columns.append(((qual_col_name, ''), desc))
                        
                        if tokens and tokens[0] == "DESC":
                            pop_and_check(tokens, "DESC")
                            
                        if not tokens:
                            break
                        pop_and_check(tokens, ",")
                    
                    
                    if for_view == True:
                        
                        return [output_columns,
                        order_by_columns,
                        from_join_clause,
                        where_clause,
                        is_distinct, aggs]
                    return self.database.select(
                        output_columns,
                        order_by_columns,
                        from_join_clause=from_join_clause,
                        where_clause=where_clause,
                        is_distinct=is_distinct, aggs=aggs)
                    
                else: # not DESC
                    order_by_columns = []
                    while True:
                        collate_function = ''
                        qual_col_name = get_qualified_column_name(tokens)
                        if tokens:
                            if tokens[0] == "COLLATE":
                                tokens.pop(0)
                                collate_function = tokens.pop(0)
                        
                        if collate_function != '':
                            order_by_columns.append(((qual_col_name, self.database.collations[collate_function]), ''))
                        else:
                            order_by_columns.append(((qual_col_name, ''), ''))
                            
                        if not tokens:
                            break
                        pop_and_check(tokens, ",")
                        
                    if for_view == True:
                        return [output_columns,
                        order_by_columns,
                        from_join_clause,
                        where_clause,
                        is_distinct, aggs]
                    
                    return self.database.select(
                        output_columns,
                        order_by_columns,
                        from_join_clause=from_join_clause,
                        where_clause=where_clause,
                        is_distinct=is_distinct, aggs=aggs)
                    
                
                
            
        
        def drop(tokens):
            
            
            
            if "exclusive" in self.database.locks:
                if self.database.locks["exclusive"] != self.unique_id:
                    raise LockInterferenceError("drop 1 cant do that cuz of ze locks")
            if "reserved" in self.database.locks:
                if self.database.locks["reserved"] != self.unique_id:
                    raise LockInterferenceError("drop 2 cant do that cuz of ze locks")
            
            
            
            if self.in_transaction == True:
                self.database.locks["exclusive"] = unique_id
                pop_and_check(tokens, "DROP")
                pop_and_check(tokens, "TABLE")
                if tokens[0] == "IF":
                    pop_and_check(tokens, "IF")
                    pop_and_check(tokens, "EXISTS")
                    table_name = tokens.pop(0)
                    if table_name not in self.local_database.tables:
                        return
                else:
                    table_name = tokens.pop(0)
                    
                del self.local_database.tables[table_name]
                
            else:
                pop_and_check(tokens, "DROP")
                pop_and_check(tokens, "TABLE")
                if tokens[0] == "IF":
                    pop_and_check(tokens, "IF")
                    pop_and_check(tokens, "EXISTS")
                    table_name = tokens.pop(0)
                    if table_name not in self.database.tables:
                        return
                else:
                    table_name = tokens.pop(0)
                    
                del self.database.tables[table_name]
        
        
        
        def begin_transaction(tokens):
    
            pop_and_check(tokens, "BEGIN")
            
            
            if tokens[0] != "TRANSACTION":
                if tokens[0] == "DEFERRED":
                    # nothing really needs to be done here. Default case.
                    pop_and_check(tokens, "DEFERRED")
                    pass
                    
                    
                elif tokens[0] == "IMMEDIATE":
                    pop_and_check(tokens, "IMMEDIATE")
                    if "exclusive" in self.database.locks:
                        raise LockInterferenceError("begin 1 cant do that cuz of ze locks")
                    elif "reserved" in self.database.locks:
                        raise LockInterferenceError("begin 2 cant do that cuz of ze locks")
                    
                    self.database.locks["reserved"] = self.unique_id
                    
                    
                elif tokens[0] == "EXCLUSIVE":
                    pop_and_check(tokens, "EXCLUSIVE")
                    if "exclusive" in self.database.locks:
                        raise LockInterferenceError("begin 3 cant do that cuz of ze locks")
                    elif "reserved" in self.database.locks:
                        raise LockInterferenceError("begin 4 cant do that cuz of ze locks")
                    elif "shared" in self.database.locks:
                        raise LockInterferenceError("begin 5 cant do that cuz of ze locks")
                    
                    self.database.locks["exclusive"] = self.unique_id
            
            
            pop_and_check(tokens, "TRANSACTION")
            
            if self.in_transaction == True:
                raise AlreadyInTransactionError("Already in a transaction")
            else: 
                self.in_transaction = True
            
            
            # create local database
            self.local_database = deepcopy(self.database)
            
            
        def commit_transaction(tokens):
            pop_and_check(tokens, "COMMIT")
            pop_and_check(tokens, "TRANSACTION")
            
            
            if "exclusive" in self.database.locks:
                if self.database.locks["exclusive"] != self.unique_id:
                    raise LockInterferenceError("commit 1 cant do that cuz of ze locks")
            #if "reserved" in self.database.locks:
            #    if self.database.locks["reserved"] != self.unique_id:
            #        raise LockInterferenceError("cant do that cuz of ze locks")
            
            
            
            
            
            
            if self.in_transaction == False:
                raise NotInTransactionError("Not in a transaction")
            else:
                self.in_transaction = False
            
            # release all locks
            for i in self.database.locks:
                if self.database.locks[i] == self.unique_id:
                    del self.database.locks[i]
                    break
                    
            self.database.tables = self.local_database.tables
        
            
            
        def rollback_transaction(tokens):
            pop_and_check(tokens, "ROLLBACK")
            pop_and_check(tokens, "TRANSACTION")
            
            if self.in_transaction == False:
                raise NotInTransactionError("Not in a transaction")
            else:
                self.in_transaction = False
            
            # release all locks
            for i in self.database.locks:
                if self.database.locks[i] == self.unique_id:
                    del self.database.locks[i]
                    break
            
            #self.local_database.tables = self.database.tables
            
            
        
        
        

        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE", "DROP", "BEGIN", "COMMIT", "ROLLBACK"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        elif tokens[0] == "SELECT":
            return select(tokens)
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        elif tokens[0] == "DROP":
            drop(tokens)
            return []
        elif tokens[0] == "BEGIN":
            begin_transaction(tokens)
            return []
        elif tokens[0] == "COMMIT":
            commit_transaction(tokens)
            return []
        elif tokens[0] == "ROLLBACK":
            rollback_transaction(tokens)
            return []
        else:
            raise AssertionError(
                "Unexpected first word in statements: " + tokens[0])

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename, timeout=0, isolation_level=None):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class QualifiedColumnName:

    def __init__(self, col_name, table_name=None):
        self.col_name = col_name
        self.table_name = table_name

    def __str__(self):
        return "QualifiedName({}.{})".format(
            self.table_name, self.col_name)

    def __eq__(self, other):
        same_col = self.col_name == other.col_name
        if not same_col:
            return False
        both_have_tables = (self.table_name is not None and
                            other.col_name is not None)
        if not both_have_tables:
            return True
        return self.table_name == other.table_name

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash((self.col_name, self.table_name))

    def __repr__(self):
        return str(self)


class Database:

    def __init__(self, filename):
        self.filename = filename
        self.tables = {}
        self.counter = 0
        self.connections = []
        self.locks = {}
        self.views = {}
        self.collations = {}



    def __deepcopy__(self, memodict={}):
        new_instance = Database(self.filename)
        new_instance.__dict__.update(self.__dict__)
        new_instance.filename = deepcopy(self.filename)
        new_instance.tables = deepcopy(self.tables)
        new_instance.counter = deepcopy(self.counter)
        return new_instance




    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []
        
        
        
    def create_view(self, output_columns, order_by_columns):
        pass
    
    def return_view():
        return self.select(output_columns, order_by_columns)
        

    def insert_into(self, table_name, row_contents, qual_col_names=None):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents, qual_col_names=qual_col_names)
        return []

    def update(self, table_name, update_clauses, where_clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.update(update_clauses, where_clause)

    def delete(self, table_name, where_clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.delete(where_clause)

    def select(self, output_columns, order_by_columns,
               from_join_clause,
               where_clause=None, is_distinct=False, aggs=[]):
                   
        if from_join_clause.left_table_name not in self.tables:
            if from_join_clause.left_table_name in self.views:
                
                # have the table that the view is against
                table = self.tables[self.views[from_join_clause.left_table_name][1]]
                table_name = self.views[from_join_clause.left_table_name][1]
                count = 0
                for i in self.views[from_join_clause.left_table_name][0]:
                    count += 1


                # create table of view results, then execute select query against that
                
                # have all of the rows that the view returns
                temp_table_rows = table.select_rows(self.views[from_join_clause.left_table_name][0][0], \
                                        self.views[from_join_clause.left_table_name][0][1], \
                                        where_clause=self.views[from_join_clause.left_table_name][0][3], \
                                        is_distinct=self.views[from_join_clause.left_table_name][0][4])
                
                # create a temp table with all the rows that the view returns
                # excecute select query on that temp table
                
                
                
                # get column types from database.tables[table]
                view_column_types = []
                for i in self.views[from_join_clause.left_table_name][0][0]:

                    index_of_column = self.tables[table_name].column_names.index(i)
                    view_column_types.append(self.tables.column_types[index_of_column])
                
                
                
                
                view_column_names = self.views[from_join_clause.left_table_name][0][0]
                
                
                # column_name_type_pairs = zip(output_columns, column_types)
                column_name_type_pairs = zip(view_column_names, view_column_types)
                                        
                self.tables[view_table] = Table(view_table, column_name_type_pairs)
                                        
                return view_table.select_rows(output_columns, order_by_columns,
               from_join_clause,
               where_clause, is_distinct)
                
                
                
                
                
                
                
                
                
                
            else:
                assert from_join_clause.left_table_name in self.tables
        if from_join_clause.right_table_name:
            assert from_join_clause.right_table_name in self.tables
            left_table = self.tables[from_join_clause.left_table_name]
            right_table = self.tables[from_join_clause.right_table_name]
            all_columns = itertools.chain(
                zip(left_table.column_names, left_table.column_types),
                zip(right_table.column_names, right_table.column_types))
                
            left_col = from_join_clause.left_join_col_name
            right_col = from_join_clause.right_join_col_name
            join_table = Table("", all_columns)
            combined_rows = []
            for left_row in left_table.rows:
                left_value = left_row[left_col]
                found_match = False
                for right_row in right_table.rows:
                    right_value = right_row[right_col]
                    if left_value is None:
                        break
                    if right_value is None:
                        continue
                    if left_row[left_col] == right_row[right_col]:
                        new_row = dict(left_row)
                        new_row.update(right_row)
                        combined_rows.append(new_row)
                        found_match = True
                        continue
                if left_value is None or not found_match:
                    new_row = dict(left_row)
                    new_row.update(zip(right_row.keys(),
                                       itertools.repeat(None)))
                    combined_rows.append(new_row)

            join_table.rows = combined_rows
            table = join_table
        else:
            table = self.tables[from_join_clause.left_table_name]
            

        return table.select_rows(output_columns, order_by_columns,
                                 where_clause=where_clause,
                                 is_distinct=is_distinct, aggs=aggs)


class Table:

    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_name_type_pairs = column_name_type_pairs
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []
        

    def __deepcopy__(self, memodict={}):
        new_instance = Table(self.name, self.column_name_type_pairs)
        new_instance.__dict__.update(self.__dict__)
        new_instance.name = deepcopy(self.name)
        new_instance.column_names = deepcopy(self.column_names)
        new_instance.column_types = deepcopy(self.column_types)
        new_instance.rows = deepcopy(self.rows)
        return new_instance
    

    def insert_new_row(self, row_contents, qual_col_names=None):
        if row_contents == []:
            for i in self.column_types:
                row_contents.append(i[1])
            
        
        if not qual_col_names:
            qual_col_names = self.column_names
        assert len(qual_col_names) == len(row_contents)
        row = dict(zip(qual_col_names, row_contents))
        for null_default_col in set(self.column_names) - set(qual_col_names):
            defualt_dict = {}
            index_of_column = self.column_names.index(null_default_col)
            if self.column_types[index_of_column][1] == '':
                row[null_default_col] = None
            else:
                row[null_default_col] = self.column_types[index_of_column][1]
        self.rows.append(row)

    def update(self, update_clauses, where_clause):
        for row in self.rows:
            if self._row_match_where(row, where_clause):
                for update_clause in update_clauses:
                    row[update_clause.col_name] = update_clause.constant

    def delete(self, where_clause):
        self.rows = [row for row in self.rows
                     if not self._row_match_where(row, where_clause)]




    def _row_match_where(self, row, where_clause):
        if not where_clause:
            return True
        new_rows = []
        value = row[where_clause.col_name]

        op = where_clause.operator
        cons = where_clause.constant
        if ((op == "IS NOT" and (value is not cons)) or
                (op == "IS" and value is cons)):
            return True

        if value is None:
            return False

        if ((op == ">" and value > cons) or
            (op == "<" and value < cons) or
            (op == "=" and value == cons) or
                (op == "!=" and value != cons)):
            return True
        return False

    def select_rows(self, output_columns, order_by_columns,
                    where_clause=None, is_distinct=False, contains_desc=False, aggs=[]):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col.col_name == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names
                       for col in columns)

        def ensure_fully_qualified(columns):
            for col in columns:
                if col.table_name is None:
                    col.table_name = self.name


        def insertion_sort(arr, function, column, rev=1):
            # -1 less than
            # 0 equal to
            # 1 greater than
            #print(arr)
            
            for i in range(len(arr)):
                cursor = arr[i]
                pos = i
                while pos > 0 and function(arr[pos - 1][column], cursor[column]) == rev:
                    arr[pos] = arr[pos - 1]
                    pos = pos - 1
                arr[pos] = cursor

            return arr
            
            
        # IF DESC THEN reversed(SORTED())
        def sort_rows(rows, order_by_columns, contains_desc):
            for i in range(len(order_by_columns)-1, -1, -1):
                if order_by_columns[i][0][1] != '':
                    mycmp = order_by_columns[i][0][1]
                    def cmp_to_key(mycmp):
                        class K:
                            def __init__(self, obj, *args):
                                self.obj = obj
                            def __lt__(self, other):
                                return mycmp(self.obj, other.obj) < 0
                            def __gt__(self, other):
                                return mycmp(self.obj, other.obj) > 0
                            def __eq__(self, other):
                                return mycmp(self.obj, other.obj) == 0
                            def __le__(self, other):
                                return mycmp(self.obj, other.obj) <= 0
                            def __ge__(self, other):
                                return mycmp(self.obj, other.obj) >= 0
                            def __ne__(self, other):
                                return mycmp(self.obj, other.obj) != 0
                        return K

                if order_by_columns[i][1] == '':

                    if order_by_columns[i][0][1] == '':
                        # sort 
                        rows = sorted(rows, key=itemgetter(order_by_columns[i][0][0]))
                    else:
                        # sort with special sort
                        rows = insertion_sort(rows, order_by_columns[i][0][1], order_by_columns[i][0][0])
                        #rows = sorted(rows, key=itemgetter(order_by_columns[i][0][0]))
                else: # DESC
                    if order_by_columns[i][0][1] == '':
                        # sort in reverse
                        rows = sorted(rows, reverse=True, key=itemgetter((order_by_columns[i][0][0])))
                    else:
                        # sort in reverse special order
                        rows = (insertion_sort(rows, order_by_columns[i][0][1], order_by_columns[i][0][0], -1))
            return rows 
                    
            #return sorted(rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        def remove_duplicates(tuples):
            seen = set()
            uniques = []
            for row in tuples:
                if row in seen:
                    continue
                seen.add(row)
                uniques.append(row)
            return uniques

        expanded_output_columns = expand_star_column(output_columns)

        check_columns_exist(expanded_output_columns)
        ensure_fully_qualified(expanded_output_columns)
        fixed_order_by_columns = []
        for i in order_by_columns:
            fixed_order_by_columns.append(i[0][0]) ##### CHANGED THIS PART TO [0][0]
        check_columns_exist(fixed_order_by_columns)
        ensure_fully_qualified(fixed_order_by_columns)

        filtered_rows = [row for row in self.rows
                         if self._row_match_where(row, where_clause)]
        
        sorted_rows = sort_rows(filtered_rows, order_by_columns, contains_desc)

        list_of_tuples = generate_tuples(sorted_rows, expanded_output_columns)
        if is_distinct:
            return remove_duplicates(list_of_tuples)
            
        if aggs != []:
            
            agg_indexes = []
            
            new_output = []
            list_of_tuples = list(list_of_tuples)
            for i in aggs:
                agg_index = output_columns.index(i[0])
                if i[1] == "_max":
                    new_output.append((max(list_of_tuples, key=itemgetter(agg_index)))[agg_index])
                else:
                    new_output.append((min(list_of_tuples, key=itemgetter(agg_index)))[agg_index])
            
            return [tuple(new_output)]
                
                
        return list_of_tuples


def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    if (query[0] == "'"):
        delimiter = "'"
    else:
        delimiter = '"'
    query = query[1:]
    end_quote_index = query.find(delimiter)
    while query[end_quote_index + 1] == delimiter:
        # Remove Escaped Quote
        query = query[:end_quote_index] + query[end_quote_index + 1:]
        end_quote_index = query.find(delimiter, end_quote_index + 1)
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[:2] == "!=":
            tokens.append(query[:2])
            query = query[2:]
            continue

        if query[0] in "(),;*.><=":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] in {"'", '"'}:
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError(
                "Query didn't get shorter. query = {}".format(query))

    return tokens

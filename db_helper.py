#!/usr/bin/env python3
"""
    Database helper functions.
    Needs sqlite3
    FIXME: This should probably help with parameterising the inputs, but mostly it's working with 
    clean inputs, so there's not much chance of meeting "Little Bobby Tables; Drop Table Users;" or similar 
"""

def dbdo(dbc, cmd, verbose):
    """
    Execute a database command, optionally printing the cmd if verbose
    """
    if verbose:
        print(cmd)

    result = dbc.execute(cmd)
    return result

def delete_named_tables(dbc, pattern, VERBOSE):
    tables_to_delete = list_from_query(dbc, 'SELECT name from sqlite_master where name like "{}"'.format(pattern))
    dbdo(dbc, 'BEGIN', VERBOSE)
    for table_name in tables_to_delete:
        dbdo(dbc, 'DROP TABLE [{}]'.format(table_name), VERBOSE)
    dbdo(dbc, 'COMMIT', VERBOSE)

    return len(tables_to_delete)

def list_from_query(dbc, query_str):
    """
    Return an list from the database.
    only uses the first column returned by the query.
    """
    results = []
    for row in dbc.execute(query_str):
        results.append(row[0])

    return results


def value_from_query(dbc, query_str):
    """
    Return a single value from a query.
    """
    print(query_str)
    results = list_from_query(dbc, query_str)
    if len(results) == 0:
        return 'Null'
    else:
        return results[0]


def dict_from_query(dbc, query_str):
    """
    Return a query as a dict of two elements:
    e.g. select name, rank from staff
    staff['Fred'] = 'Boss'
    Only uses the first two columns of the query.
    """
    results = {}
    for row in dbc.execute(query_str):
        results[row[0]] = row[1]

    return results


def row_from_query(dbc, query_str):
    """
    Return a single row from the database.
    uses the first row returned by the query.
    """
    results = []
    for row in dbc.execute(query_str):
        results.append(row)

    if len(results) > 0:
        return results[0]
    else:
        return None


def rows_from_query(dbc, query_str):
    """
    Return a list of lists from a database query.
    each list contains a list of all the rows.
    """
    results = []
    for row in dbc.execute(query_str):
        results.append(row)

    return results

def make_tables_from_dict(dbc, tabledefs, VERBOSE):
    # Make the Database tables
    print ('Dropping and Building Tables...')
    for table in tabledefs.keys():
        result = dbc.execute('DROP TABLE IF EXISTS [{}]'.format(table))
        sql_cmd = 'CREATE TABLE [{}] ({});'.format(table, tabledefs[table])
        if VERBOSE:
            print(sql_cmd)
        result = dbc.execute(sql_cmd)

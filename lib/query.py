del_query_1_key = """
    delete from {param.table_name} where {param.key_col} = {param.key_val}
"""
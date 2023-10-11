def write_brightway2_database(data, name, reset_codes=False):
    # Restore parameters to Brightway2 format
    # which allows for uncertainty and comments
    change_db_name(data, name)
    if reset_codes:
        reset_all_codes(data)
    link_internal(data)
    check_internal_linking(data)
    PremiseImporter(name, data).write_database()

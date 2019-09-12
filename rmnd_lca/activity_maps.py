
material_filters = {
    "steel": {
        "fltr": "market for steel,",
        "mask": "hot rolled"},
    "concrete": {"fltr": "market for concrete,"},
    "copper": {
        "fltr": "market for copper",
        "filter_exact": True},
    "aluminium": {
        "fltr": ["market for aluminium, primary",
                 "market for aluminium alloy,"]},
    "electricity": {"fltr": "market for electricity"},
    "gas": {
        "fltr": "market for natural gas,",
        "mask": ["network", "burned"]},
    "diesel": {
        "fltr": "market for diesel",
        "mask": ["burned", "electric"]},
    "petrol": {
        "fltr": "market for petrol,",
        "mask": "burned"},
    "freight": {"fltr": "market for transport, freight"},
    "cement": {"fltr": "market for cement,"},
    "heat": {"fltr": "market for heat,"}
}


def act_fltr(db, fltr={}, mask={}, filter_exact=False, mask_exact=False):
    """Filter `db` for activities matching field contents given by `fltr` excluding strings in `mask`.
    `fltr`: string, list of strings or dictionary.
    If a string is provided, it is used to match the name field from the start (*startswith*).
    If a list is provided, all strings in the lists are used and results are joined (*or*).
    A dict can be given in the form <fieldname>: <str> to filter for <str> in <fieldname>.
    `mask`: used in the same way as `fltr`, but filters add up with each other (*and*).
    `filter_exact` and `mask_exact`: boolean, set `True` to only allow for exact matches.

    :param db: A lice cycle inventory database
    :type db: brightway2 database object
    :param fltr: value(s) to filter with.
    :type fltr: Union[str, lst, dict]
    :param mask: value(s) to filter with.
    :type mask: Union[str, lst, dict]
    :param filter_exact: requires exact match when true.
    :type filter_exact: bool
    :param mask_exact: requires exact match when true.
    :type mask_exact: bool
    :return: list of activity data set names
    :rtype: list

    """
    result = []

    # default field is name
    if type(fltr) == list or type(fltr) == str:
        fltr = {
            "name": fltr
        }
    if type(mask) == list or type(mask) == str:
        mask = {
            "name": mask
        }

    def like(a, b):
        if filter_exact:
            return a == b
        else:
            return a.startswith(b)

    def notlike(a, b):
        if mask_exact:
            return a != b
        else:
            return b not in a

    assert len(fltr) > 0, "Filter dict must not be empty."
    for field in fltr:
        condition = fltr[field]
        if type(condition) == list:
            for el in condition:
                # this is effectively connecting the statements by *or*
                result.extend([act for act in db if like(act[field], el)])
        else:
            result.extend([act for act in db if like(act[field], condition)])

    for field in mask:
        condition = mask[field]
        if type(condition) == list:
            for el in condition:
                # this is effectively connecting the statements by *and*
                result = [act for act in result if notlike(act[field], el)]
        else:
            result = [act for act in result if notlike(act[field], condition)]
    return result


def generate_sets_from_filters(db):
    """Generate sets of activity names for technologies from the filter specifications.

    :param db: A life cycle inventory database
    :type db: brightway2 database object
    :return: dictionary with material types as keys and list of activity data set names as values.
    :rtype: dict
    """
    techs = {tech: act_fltr(db, **fltr) for tech, fltr in material_filters.items()}
    return {tech: set([act["name"] for act in actlst]) for tech, actlst in techs.items()}
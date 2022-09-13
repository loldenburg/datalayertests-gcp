def is_populated(var):
    if var is None:
        return False
    elif var == "":
        return False
    elif type(
            var) == str:  # previous condition is already enough: If it is not "" and it is a string, it cannot be empty
        return True
    elif type(var) == list:
        if len(var) == 0:
            return False


def safe_get_index(l, idx, default=None):
    """Safely gets a list index, e.g. listname[4] and returns the default if not found.
    :param l: list to check
    :type l: list
    :param idx: index to check
    :param default: Default value to return if index doesn't exist
    :return: value of index or default
    """
    if isinstance(l, list) is False:
        return default
    try:
        return l[idx]
    except IndexError:
        return default

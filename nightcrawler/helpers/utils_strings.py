# equals


def check_string_equals_substring(string: str, substring: str) -> bool:
    """Check whether string equals substring.

    Args:
        string (str): string to check.
        substring (str): substring to check.

    Returns:
        bool: whether string equals substring.
    """

    return substring == string


def check_string_equals_any_substring(string: str, substrings: list[str]) -> bool:
    """Check whether string equals any substring.

    Args:
        string (str): string to check.
        substrings (list[str]): substrings to check.

    Returns:
        bool: whether string equals any substring.
    """

    for substring in substrings:
        if check_string_equals_substring(string, substring):
            return True
    return False


def check_any_string_equals_any_substring(
    strings: list[str], substrings: list[str]
) -> bool:
    """Check whether any string equals any substring.

    Args:
        strings (list[str]): strings to check.
        substrings (list[str]): substrings to check.

    Returns:
        bool: whether any string equals any substring.
    """

    for string in strings:
        if check_string_equals_any_substring(string, substrings):
            return True
    return False


# contains


def check_string_contains_substring(string: str, substring: str) -> bool:
    """Check whether string contains substring.

    Args:
        string (str): string to check.
        substring (str): substring to check.

    Returns:
        bool: whether string contains substring.
    """

    return substring in string


def check_string_contains_any_substring(string: str, substrings: list[str]) -> bool:
    """Check whether string contains any substring.

    Args:
        string (str): string to check.
        substrings (list[str]): substrings to check.

    Returns:
        bool: whether string contains any substring.
    """

    for substring in substrings:
        if check_string_contains_substring(string, substring):
            return True
    return False

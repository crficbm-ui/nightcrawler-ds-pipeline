import os


# data


def compose_path_dataset_file(path_data: str, country: str, file_name: str) -> str:
    """Compose path to dataset file.

    Args:
        path_data (str): path to data.
        country (str): country.
        file_name (str): name of the file.
    """

    return os.path.join(path_data, country, f"{file_name}.csv")


# settings


def compose_path_setting_file(
    path_settings: str, country: str, filterer_name: str
) -> str:
    """Compose path to setting file.

    Args:
        path_settings (str): path to settings.
        country (str): country.
        file_name (str): name of the file.
    """

    return os.path.join(path_settings, country, f"{filterer_name}.yaml")

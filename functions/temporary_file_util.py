__tmp_file = {}


def put_temp_file(file_name, tmp_file):
    """

    :param file_name:
    :param tmp_file:
    :return:
    """
    global __tmp_file
    __tmp_file[file_name] = tmp_file


def get_temp_file(file_name):
    """

    :return:
    """
    global __tmp_file
    return __tmp_file[file_name]

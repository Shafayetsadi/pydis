DELIMITER: str = "\r\n"


def parse_bulk_string(data: str, start=0) -> tuple:
    """
    Parse a bulk string from RESP (REdis Serialization Protocol) data.

    Args:
        data (str): The RESP data to parse.
        start (int, optional): The starting index of the bulk string in the data. Defaults to 0.

    Returns:
        tuple: A tuple containing the parsed bulk string and the index of the end of the bulk string in the data.

    Raises:
        ValueError: If the data does not start with a '$', indicating invalid bulk string data.
    """
    if data[start] != '$':
        raise ValueError("Invalid bulk string data")
    index = data.find(DELIMITER, start)
    length = int(data[start + 1:index])  # length of the bulk string
    if length == -1:
        return "", index + len(DELIMITER)
    end = index + len(DELIMITER) + length
    return data[index + len(DELIMITER):end], end + len(DELIMITER)


def parse_array(data: str) -> list:
    """
    Parse an array from RESP (REdis Serialization Protocol) data.

    Args:
        data (str): The RESP data to parse.

    Returns:
        list: The parsed array.

    Raises:
        ValueError: If the data does not start with a '*', indicating invalid array data.
    """
    if data[0] != '*':
        raise ValueError("Invalid array data")
    index = data.find(DELIMITER)
    length = int(data[1:index])  # length of the array
    result = []
    start = index + len(DELIMITER)
    for _ in range(length):
        element, start = parse_bulk_string(data, start)
        result.append(element)
    return result


if __name__ == "__main__":
    bulk_data = "$3\r\nhey\r\n"
    try:
        print(parse_bulk_string(bulk_data))
    except ValueError as e:
        print(e)

    array_data = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    try:
        print(parse_array(array_data))
    except ValueError as e:
        print(e)

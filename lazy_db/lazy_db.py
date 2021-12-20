"""Main module."""
import json
import io
from pathlib import Path
from typing import Dict, List, Union, Tuple


class IndexingError(Exception):
    pass


class LazyDb:
    def __init__(self, file: str, int_size: int = 4, key_int_size: int = 4, content_int_size: int = 4):
        """Opens database and sets specified db settings if bootstrapping a new database"""
        path = Path(file)
        if not path.is_file():
            path.touch()
            self.int_size = int_size
            self.key_int_size = key_int_size
            self.content_int_size = content_int_size
            self.f = open(file, "rb+")
            self.write_info()
            self.headers = {}
            return

        self.f = open(file, "rb+")
        info = self.get_info()
        self.int_size = info["int_size"]
        self.key_int_size = info["key_int_size"]
        self.content_int_size = info["content_int_size"]
        self.headers = self.get_headers()

    def get_headers(self) -> Dict[Union[str, int], int]:
        """Gets all headers from entries in database"""
        headers_out = {}

        self.f.seek(0, io.SEEK_SET)
        self.seek_to(b"\x00")

        while True:
            if not self.f.read(1) == b"\x00":
                break
            key_type = self.f.read(1)
            if key_type == b"\x01":
                key_bytes = self.read_to(b"\x00")
                key = key_bytes.decode("utf-8")
            elif key_type == b"\x02":
                key_bytes = self.f.read(self.key_int_size)
                self.f.seek(1, io.SEEK_CUR)
                key = int.from_bytes(key_bytes, "little")
            else:
                raise IndexingError(f"Encountered a problem while indexing: key header byte {key_type} is not a"
                                    f"supported header byte. Your database may be corrupted.")
            headers_out[key] = self.f.tell()

            length_bytes = self.f.read(self.content_int_size)
            length = int.from_bytes(length_bytes, "little")
            self.f.seek(length, io.SEEK_CUR)

        return headers_out

    def read(self, key: Union[str, int]) -> Union[str, int, List[Union[str, int, dict]], Dict[Union[str, int], Union[str, int, list, dict]], None]:
        location = self.headers.get(key, None)

        if location is None:
            return None

        self.f.seek(location, io.SEEK_SET)
        length_bytes = self.f.read(self.content_int_size)
        length = int.from_bytes(length_bytes, "little")

        content_bytes = self.f.read(length)
        content = self.from_bytes(content_bytes)

        return content

    def get_info(self) -> dict:
        """Gets db info"""
        self.f.seek(0, io.SEEK_SET)
        data_bytes = self.read_to(b"\x00")
        info_dict = self.bytes_to_dict(data_bytes)
        return info_dict

    def read_to(self, to_byte: bytes) -> bytes:
        """Reads to specified byte"""
        out_array = bytearray()
        while byte := self.f.read(1):
            if byte == to_byte:
                break
            out_array += byte
        return bytes(out_array)

    def seek_to(self, to_byte: bytes) -> bool:
        """Seeks to specified byte"""
        while byte := self.f.read(1):
            if byte == to_byte:
                return True
            if byte == b"":
                return False

    def write_info(self):
        """Writes db info. Only should be called when a new database is bootstrapped"""
        info = {"int_size": self.int_size,
                "key_int_size": self.key_int_size,
                "content_int_size": self.content_int_size}
        info_str = json.dumps(info, separators=(',', ':'))
        self.f.write(self.str_to_bytes(info_str, add_type_header=False) + b"\x00")

    def bytes_to_str(self, bytes_in: bytes) -> str:
        """Convert bytes to string"""
        return bytes_in.decode("utf-8")

    def str_to_bytes(self, string_in: str, add_type_header: bool = True) -> bytes:
        """Convert a string to bytes"""
        data = string_in.encode("utf-8")
        if add_type_header:
            return b"\x01" + data
        return data

    def bytes_to_int(self, bytes_in: bytes) -> int:
        """Convert bytes to int"""
        return int.from_bytes(bytes_in, 'little')

    def int_to_bytes(self, integer_in: int, add_type_header: bool = True) -> bytes:
        """Convert an integer to bytes"""
        data = integer_in.to_bytes(self.int_size, 'little')
        if add_type_header:
            return b"\x02" + data
        return data

    def bytes_to_dict(self, bytes_in: bytes) -> dict:
        """Convert bytes to dict"""
        str_value = self.bytes_to_str(bytes_in)
        return json.loads(str_value)

    def dict_to_bytes(self, dict_in: dict, add_type_header: bool = True) -> bytes:
        """Convert dictionary to bytes"""
        json_str = json.dumps(dict_in, separators=(',', ':'))
        data = self.str_to_bytes(json_str, add_type_header=False)
        if add_type_header:
            return b"\x03" + data
        return data

    def bytes_to_list(self, bytes_in: bytes) -> list:
        """Convert bytes to list"""
        raise NotImplementedError("lists are not yet implemented")

    def list_to_bytes(self, list_in: list, add_type_header: bool = True) -> bytes:
        """Convert list to bytes"""
        raise NotImplementedError("lists are not yet implemented")
        # return b"\x04" + b"test"

    def from_bytes(self, bytes_data: bytes) -> Union[str, int, List[Union[str, int, dict]], Dict[Union[str, int], Union[str, int, list, dict]]]:
        """Converts a given bytes value to object form"""
        bytes_type = bytes_data[0]
        bytes_data = bytes_data[1:]
        if bytes_type == 1:
            return self.bytes_to_str(bytes_data)
        if bytes_type == 2:
            return self.bytes_to_int(bytes_data)
        if bytes_type == 3:
            return self.bytes_to_dict(bytes_data)
        if bytes_type == 4:
            return self.bytes_to_list(bytes_data)
        raise TypeError(f"Incorrect type header byte: {bytes_type}. Your database may be corrupted.")

    def to_bytes(self, value: Union[str, int, List[Union[str, int, dict]], Dict[Union[str, int], Union[str, int, list, dict]]], add_type_header: bool = True) -> bytes:
        """Converts a given object to byte form"""
        if isinstance(value, str):
            return self.str_to_bytes(value)
        if isinstance(value, int):
            return self.int_to_bytes(value)
        if isinstance(value, list):
            return self.list_to_bytes(value)
        if isinstance(value, dict):
            return self.dict_to_bytes(value)
        raise TypeError(f"{type(value)} is not a supported content type to be written to the database.")

    def gen_header(self, key: Union[str, int], data: bytes) -> Tuple[bytes, int]:
        """Generate header and get content offset"""
        content_len = len(data)
        # int_to_bytes is not used here since we want to define the int size separately from content, as well as we
        # don't need the type identifier byte
        content_len_bytes = content_len.to_bytes(self.content_int_size, 'little')
        # same idea with the key bytes, except we do need the type identifier byte since it could be a int or a string
        if isinstance(key, str):
            key_bytes = self.to_bytes(key)
        elif isinstance(key, int):
            key_bytes = b"\x02" + key.to_bytes(self.key_int_size, 'little')
        else:
            raise TypeError(f"{type(key)} is not a supported key type to be written to the database.")

        content_offset = len(key_bytes) + 2
        return b"\x00" + key_bytes + b"\x00" + content_len_bytes, content_offset

    def write_raw_bytes(self, data: bytes) -> int:
        """Writes raw bytes to database and returns the location it was written to"""
        self.f.seek(0, io.SEEK_END)
        location = self.f.tell()
        self.f.write(data)
        return location

    def write_bytes(self, key: Union[str, int], data: bytes) -> int:
        """Writes bytes to database under key and returns location here content starts"""
        if key in self.headers:
            raise KeyError(f"Key {key} is already in the database")
        header, content_offset = self.gen_header(key, data)
        write_location = self.write_raw_bytes(header + data)
        return write_location + content_offset

    def write(self, key: Union[str, int], value: Union[str, int, List[Union[str, int, dict]], Dict[Union[str, int], Union[str, int, list, dict]]]):
        """Writes a string, list, integer, or a dict to the database."""
        data = self.to_bytes(value)
        content_location = self.write_bytes(key, data)
        self.headers[key] = content_location

    def close(self):
        """Close the database"""
        self.f.close()

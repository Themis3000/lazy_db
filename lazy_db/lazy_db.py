"""Main module."""
import json
import io
import math
from pathlib import Path
from typing import Dict, List, Union, Tuple


class IndexingError(Exception):
    pass


class LazyDb:
    def __init__(self, file: str, key_int_size: int = 4, content_int_size: int = 4, int_list_size: int = 4):
        """Opens database and sets specified db settings if bootstrapping a new database"""
        path = Path(file)
        if not path.is_file():
            path.touch()
            self.key_int_size = key_int_size
            self.content_int_size = content_int_size
            self.int_list_size = int_list_size
            self.f = open(file, "rb+")
            self.write_info()
            self.headers = {}
            return

        self.f = open(file, "rb+")
        info = self.get_info()
        self.key_int_size = info["key_int_size"]
        self.content_int_size = info["content_int_size"]
        self.int_list_size = info["int_list_size"]
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
                key = self.bytes_to_str(key_bytes)
            elif key_type == b"\x02":
                key_bytes = self.f.read(self.key_int_size)
                self.f.seek(1, io.SEEK_CUR)
                key = self.bytes_to_int(key_bytes)
            else:
                raise IndexingError(f"Encountered a problem while indexing: key header byte {key_type} is not a"
                                    f"supported header byte. Your database may be corrupted.")
            headers_out[key] = self.f.tell()

            length_bytes = self.f.read(self.content_int_size)
            length = self.bytes_to_int(length_bytes)
            self.f.seek(length, io.SEEK_CUR)

        return headers_out

    def read_len(self, key: Union[str, int]) -> Union[int, None]:
        """Reads the content length of a given key and leaves the file pointer at the end of the length bytes"""
        location = self.headers.get(key, None)

        if location is None:
            raise KeyError("No entry found for that key")

        self.f.seek(location, io.SEEK_SET)
        length_bytes = self.f.read(self.content_int_size)
        return self.bytes_to_int(length_bytes)

    def read(self, key: Union[str, int]) -> Union[str, int, List[Union[str, int, dict]], Dict[Union[str, int], Union[str, int, list, dict]], None]:
        """Gets a value"""
        length = self.read_len(key)

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
        info = {"key_int_size": self.key_int_size,
                "content_int_size": self.content_int_size,
                "int_list_size": self.int_list_size}
        info_str = self.dict_to_bytes(info, add_type_header=False)
        self.f.write(info_str + b"\x00")

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

    def int_to_bytes(self, integer_in: int, add_type_header: bool = True, length: int = None) -> bytes:
        """Convert an integer to bytes"""
        if length is None:
            # Calculates the least amount of bytes the integer can be fit into
            length = math.ceil(math.log(integer_in)/math.log(256))

        data = integer_in.to_bytes(length, 'little')
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

    def bytes_to_int_list(self, bytes_in: bytes) -> list:
        """Convert bytes to int list"""
        list_out = []
        byte_groups = len(bytes_in) // self.int_list_size
        for i in range(byte_groups):
            start = i * self.int_list_size
            group = bytes_in[start:start + self.int_list_size]
            int_decoded = self.bytes_to_int(group)
            list_out.append(int_decoded)
        return list_out

    def int_list_to_bytes(self, list_in: List[int], add_type_header: bool = True) -> bytes:
        """Convert int list to bytes"""
        out_array = bytearray()
        for value in list_in:
            value_bytes = self.int_to_bytes(value, add_type_header=False, length=self.int_list_size)
            out_array.extend(value_bytes)

        if add_type_header:
            out_array.insert(0, 4)
            return bytes(out_array)
        return bytes(out_array)

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
            return self.bytes_to_int_list(bytes_data)
        if bytes_type == 5:
            return bytes_data
        raise TypeError(f"Incorrect type header byte: {bytes_type}. Your database may be corrupted.")

    def to_bytes(self, value: Union[str, bytes, int, List[Union[str, int, dict]], Dict[Union[str, int], Union[str, int, list, dict]]], add_type_header: bool = True) -> bytes:
        """Converts a given object to byte form"""
        if isinstance(value, str):
            return self.str_to_bytes(value, add_type_header=add_type_header)
        if isinstance(value, int):
            return self.int_to_bytes(value, add_type_header=add_type_header)
        if isinstance(value, list):
            return self.int_list_to_bytes(value, add_type_header=add_type_header)
        if isinstance(value, dict):
            return self.dict_to_bytes(value, add_type_header=add_type_header)
        if isinstance(value, bytes):
            if add_type_header:
                return b"\x05" + value
            return value
        raise TypeError(f"{type(value)} is not a supported content type to be written to the database.")

    def gen_header(self, key: Union[str, int], data: bytes) -> Tuple[bytes, int]:
        """Generate header and get content offset"""
        content_len = len(data)
        # int_to_bytes is not used here since we want to define the int size separately from content, as well as we
        # don't need the type identifier byte
        content_len_bytes = self.int_to_bytes(content_len, add_type_header=False, length=self.content_int_size)
        if isinstance(key, str):
            key_bytes = self.str_to_bytes(key)
        elif isinstance(key, int):
            key_bytes = self.int_to_bytes(key, length=self.key_int_size)
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

    def write(self, key: Union[str, int], value: Union[str, bytes, int, List[Union[str, int, dict]], Dict[Union[str, int], Union[str, int, list, dict]]]):
        """Writes a string, list, integer, or a dict to the database."""
        data = self.to_bytes(value)
        content_location = self.write_bytes(key, data)
        self.headers[key] = content_location

    def reconstruct_header_size(self, key: Union[str, int]):
        if isinstance(key, str):
            key_size = len(self.str_to_bytes(key, add_type_header=False))
        else:
            key_size = self.key_int_size
        # Adds the size of a content int, the size of the key, and 3 for the 3 NUL byte separators in order to figure
        # out the header length in bytes
        return self.content_int_size + key_size + 3

    def delete(self, key: Union[str, int]):
        """Deletes a entry from the database"""
        content_len = self.read_len(key)
        entry_location = self.f.tell()
        header_len = self.reconstruct_header_size(key)
        entry_len = header_len + content_len

        # Seeks to the end of the entry to be deleted
        self.f.seek(content_len, io.SEEK_CUR)

        # Steps all preceding data backwards to overwrite the deleted entry
        while data := self.f.read(1024):
            read_end = self.f.tell()
            back_bytes = len(data) + entry_len
            self.f.seek(back_bytes * -1, io.SEEK_CUR)
            self.f.write(data)
            self.f.seek(read_end, io.SEEK_SET)

        # Cuts off the now unused end of the file
        self.f.seek(0, io.SEEK_END)
        end = self.f.tell()
        self.f.truncate(end - entry_len)

        # Deletes entry key from cache
        del self.headers[key]

        # Corrects locations of entries
        for key, location in self.headers.items():
            if location >= entry_location:
                self.headers[key] = location - entry_len

    def close(self):
        """Close the database"""
        self.f.close()

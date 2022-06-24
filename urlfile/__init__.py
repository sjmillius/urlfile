__all__ = ['BufferedUrlFile', 'UrlFile', 'HTTPRangeRequestUnsupported']

import cachetools
import os
import requests


class HTTPRangeRequestUnsupported(Exception):
  pass


class UrlFile:
  '''A random access file backed by http range requests.'''

  def __init__(self, url: str):
    self._pos = 0

    # Defined properties.
    self.url: str = url
    self.closed: bool = False

    # Make a head request to get length and see whether range requests are even supported.
    head = requests.head(url=url)
    head.raise_for_status()
    self.length: int = int(head.headers['Content-Length'])

    if 'bytes' not in head.headers.get('Accept-Ranges', 'none'):
      raise HTTPRangeRequestUnsupported('http range requests not supported.')

  @property
  def mode(self) -> str:
    # Opening mode, always read-only.
    return 'rb'

  @property
  def name(self) -> str:
    return self.url

  def readable(self) -> bool:
    return True

  def seekable(self) -> bool:
    return True

  def writeable(self) -> bool:
    return True

  def close(self):
    pass

  def seek(self, offset: int, whence: int = os.SEEK_SET):
    if whence == os.SEEK_SET:
      self._pos = offset
    elif whence == os.SEEK_CUR:
      self._pos += offset
    else:
      assert whence == os.SEEK_END
      self._pos = self.length + offset

  def tell(self) -> int:
    return self._pos

  def read(self, size: int = -1) -> bytes:
    size = size if size > 0 else self.length - self._pos
    data = self._data(start=self._pos, size=size)
    self._pos += size
    return data

  # Convenience do-nothing methods.
  def __enter__(self) -> 'UrlFile':
    return self

  def __exit__(self):
    pass

  def _data(self, start: int, size: int) -> bytes:
    '''Gets data for a specific range.'''
    return self._fetch_data_range(start=start, end=start + size - 1)

  def _fetch_data_range(self, start: int, end: int) -> bytes:
    '''Fetches a data range from the remote.'''
    response = requests.get(
        url=self.url,
        headers={'Range': f'bytes={start}-{min(self.length-1, end)}'})
    response.raise_for_status()
    return response.content


class BufferedUrlFile(UrlFile):
  '''A buffered and cached UrlFile.'''

  def __init__(self,
               url: str,
               chunk_size_bytes: int = 1024 * 1024,
               cache_size_bytes: int = 10 * 1024 * 1024):
    super().__init__(url=url)
    self._chunk_size: int = chunk_size_bytes
    self._cache: cachetools.LRUCache = cachetools.LRUCache(
        maxsize=cache_size_bytes, getsizeof=lambda _: chunk_size_bytes)

  @cachetools.cachedmethod(lambda self: self._cache)
  def _fetch_chunk(self, start: int) -> bytes:
    return self._fetch_data_range(start=start, end=start + self._chunk_size - 1)

  def _data(self, start: int, size: int) -> bytes:
    '''Gets data for a specific range.'''
    buffer = b''
    offset = start % self._chunk_size
    for chunk_start in range(start - offset, start + size, self._chunk_size):
      buffer += self._fetch_chunk(start=chunk_start)
    return buffer[offset:offset + size]

__all__ = ['BufferedUrlFile', 'UrlFile', 'HTTPRangeRequestUnsupported']

import cachetools
import os
import requests
from typing import Dict


class HTTPRangeRequestUnsupported(Exception):
  pass


class UrlFile:
  '''A random access file backed by http range requests.'''

  def __init__(
      self,
      url: str,
      session: requests.Session = None,
      chunk_size_bytes: int = 1024 * 1024,
  ):
    self._pos: int = 0
    self._total_bytes_fetched: int = 0
    self._num_requests: int = 0
    self._chunk_size: int = chunk_size_bytes
    self.url: str = url
    self.session: requests.Session = session or requests.Session()

    # Make a head request to get length and see whether range requests are even supported.
    head = self.session.head(url=url)
    head.raise_for_status()
    self.length: int = int(head.headers['Content-Length'])

    if 'bytes' not in head.headers.get('Accept-Ranges', 'none'):
      raise HTTPRangeRequestUnsupported('http range requests not supported.')

  @property
  def total_bytes_fetched(self) -> int:
    return self._total_bytes_fetched

  @property
  def num_requests(self) -> int:
    return self._num_requests

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

  @property
  def closed(self) -> bool:
    return False

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

  def _range_request(self, start: int, end: int) -> Dict[str, str]:
    end = min(self.length - 1, end)
    self._num_requests += 1
    self._total_bytes_fetched += (end - start + 1)
    return {'Range': f'bytes={start}-{end}'}

  def _fetch_data_range(self, start: int, end: int):
    '''Fetches a data range from the remote.'''
    response = self.session.get(url=self.url,
                                headers=self._range_request(start=start,
                                                            end=end),
                                stream=True)
    response.raise_for_status()
    return response.iter_content(chunk_size=self._chunk_size)

  def _data(self, start: int, size: int) -> bytes:
    '''Gets data for a specific range.'''
    return b''.join(self._fetch_data_range(start=start, end=start + size - 1))


class BufferedUrlFile(UrlFile):
  '''A buffered and cached UrlFile.'''

  def __init__(self,
               url: str,
               session: requests.Session = None,
               chunk_size_bytes: int = 1024 * 1024,
               cache_size_bytes: int = 10 * 1024 * 1024):
    super().__init__(url=url,
                     session=session,
                     chunk_size_bytes=chunk_size_bytes)
    self._cache: cachetools.LRUCache = cachetools.LRUCache(
        maxsize=cache_size_bytes, getsizeof=lambda _: chunk_size_bytes)

  def _fetch_and_cache(self, start: int, end: int) -> bytes:
    buffer = b''
    for i, chunk in enumerate(self._fetch_data_range(start=start, end=end)):
      self._cache[start + i * self._chunk_size] = chunk
      buffer += chunk
    return buffer

  def _align(self, start: int) -> int:
    return start - (start % self._chunk_size)

  def _data(self, start: int, size: int) -> bytes:
    '''Gets data for a specific range.'''
    # Align start to chunk boundary.
    chunk_start = self._align(start=start)
    end = start + size
    offset = start - chunk_start

    buffer = b''

    next_start = chunk_start
    while next_start < end:
      # Fill up with everything that is in the cache.
      if next_start in self._cache:
        buffer += self._cache[next_start]
        next_start += self._chunk_size
        continue

      # Find next start of a chunk that's already in the cache.
      range_end = next_start + self._chunk_size
      while range_end < end and range_end not in self._cache:
        range_end += self._chunk_size

      # Fetch the next blob.
      buffer += self._fetch_and_cache(start=next_start, end=range_end - 1)

      next_start = range_end

    return buffer[offset:offset + size]

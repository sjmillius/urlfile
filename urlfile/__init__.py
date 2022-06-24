__all__ = ['UrlFile', 'HTTPRangeRequestUnsupported']

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
    data = self._fetch(start=self._pos,
                       end=(self._pos + size - 1) if size > 0 else self.length -
                       1).content
    return data

  # Convenience do-nothing methods.
  def __enter__(self) -> 'UrlFile':
    return self

  def __exit__(self):
    pass

  def _fetch(self, start: int, end: int) -> requests.Response:
    response = requests.get(url=self.url,
                            headers={'Range': f'bytes={start}-{end}'})
    response.raise_for_status()
    return response

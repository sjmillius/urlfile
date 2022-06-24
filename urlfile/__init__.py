import requests


class BufferedUrlFile:
  '''A random access file backed with (buffered and cached) http range requests.'''

  def __init__(self, url: str):
    self.url = url
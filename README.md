# urlfile

Lazily reading a file from a url.
Allows random access via http range requests so that not the whole file has to be downloaded first.

## Example Usage

```python
import urlfile
import zipfile

with zipfile.ZipFile(urlfile.UrlFile(url=...)) as f:
  f.printdir()
```

## Caching
Buffering and caching is provided via `urlfile.BufferedUrlFile`.

```python
f = urlfile.BufferedUrlFile(url=..., cache_size_bytes=...)
```

By default, uses a cache size of `10MB`.
## Other options
These are arguments of `UrlFile`/`BufferedUrlFile`
* ```verbose```: whether to show progress bars during fetching of data (using `rich.progress`, default: `False`)
* ```session```: a `requests.Session` to use (default: `None`, creates a new session) 
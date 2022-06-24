# urlfile

Lazily reading a file from a url.
Allows random access via http range requests so that not the whole file has to be downloaded first.

## Example Usage

```python
import urlfile
import zipfile

with zip.ZipFile(urlfile.UrlFile(url=...)) as f:
  f.printdir()
```
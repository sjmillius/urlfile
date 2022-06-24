# urlfile

Lazily reading a file from a url.
Allows random access via http range requests so that not the whole file has to be downloaded first.

## Example Usage

```python
import urlfile
import zipfile

with zip.ZipFile(urlfile.BufferedUrlFile(url=...)) as f:
  print(f.namelist())

```
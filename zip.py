import sys
import zipfile

z = zipfile.ZipFile(sys.argv[1], 'w')
z.write(sys.argv[2])


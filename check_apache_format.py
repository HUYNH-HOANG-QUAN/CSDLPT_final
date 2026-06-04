import glob
import re
from pathlib import Path

pat = re.compile(r'^(?P<ip>\S+) \S+ \S+ \[(?P<time>[^\]]+)\] "(?P<method>[A-Z]+) (?P<path>[^"]*) HTTP/(?P<http>\d\.\d)" (?P<status>\d{3}) (?P<size>\S+)(?: "(?P<ref>[^"]*)" "(?P<agent>[^"]*)"(?: \S+)?)?$')

for fn in sorted(glob.glob('*_access.log')):
    total = 0
    ok = 0
    with open(fn, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            total += 1
            if pat.match(line.strip()):
                ok += 1
    print(f'{fn}: {ok}/{total} lines match Apache combined format ({ok/total:.1%})')

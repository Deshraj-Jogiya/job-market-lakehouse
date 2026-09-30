#!/usr/bin/env python3
"""Hadoop Streaming mapper: one raw posting CSV line in, "<month>|<source>\t1" out. The header line is skipped."""
import sys


def map_line(line):
    parts = line.rstrip("\n").split(",")
    if len(parts) != 6 or not parts[0].isdigit():
        return None  # header or malformed line
    month = parts[4][:7]
    return f"{month}|{parts[3]}\t1"


if __name__ == "__main__":
    for line in sys.stdin:
        out = map_line(line)
        if out:
            print(out)

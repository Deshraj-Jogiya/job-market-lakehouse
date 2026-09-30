#!/usr/bin/env python3
"""Hadoop Streaming reducer: input arrives sorted by key, so counts are summed one key at a time."""
import sys


def reduce_lines(lines):
    current, total = None, 0
    for line in lines:
        key, value = line.rstrip("\n").split("\t")
        if key != current:
            if current is not None:
                yield f"{current}\t{total}"
            current, total = key, 0
        total += int(value)
    if current is not None:
        yield f"{current}\t{total}"


if __name__ == "__main__":
    for out in reduce_lines(sys.stdin):
        print(out)

"""Write one section of results/SUMMARY.md without touching any other section.

A section runs from its "## " heading to the next "## " heading or the end of the file.
Replacing it leaves every other section byte-for-byte as it was; a new section is
appended at the end.
"""
from fractions import Fraction


def replace_section(summary_path, heading, text):
    with open(summary_path) as fh:
        current = fh.read()
    marker = "\n" + heading + "\n"
    if marker in current:
        start = current.index(marker)
        nxt = current.find("\n## ", start + len(marker))
        tail = current[nxt:] if nxt != -1 else ""
        updated = current[:start].rstrip("\n") + "\n\n" + text.rstrip("\n") + "\n" + tail
    else:
        updated = current.rstrip("\n") + "\n\n" + text
    with open(summary_path, "w") as fh:
        fh.write(updated)


def exact_or_prefix(value, places=10):
    """Exact decimal for a terminating Fraction; otherwise its first `places` decimals
    followed by '...', every digit shown being exact (truncated, not rounded)."""
    fr = Fraction(value)
    den = fr.denominator
    for p in (2, 5):
        while den % p == 0:
            den //= p
    sign = "-" if fr < 0 else ""
    fr = abs(fr)
    if den == 1:
        k = 0
        while (fr * 10 ** k).denominator != 1:
            k += 1
        scaled = fr.numerator * 10 ** k // fr.denominator
        digits = str(scaled).rjust(k + 1, "0")
        whole, frac = digits[:len(digits) - k], digits[len(digits) - k:].rstrip("0")
        return sign + whole + ("." + frac if frac else "")
    scaled = fr.numerator * 10 ** places // fr.denominator
    digits = str(scaled).rjust(places + 1, "0")
    return sign + digits[:len(digits) - places] + "." + digits[len(digits) - places:] + "..."

import hashlib
from pathlib import Path

from .models import FinancialSeedManifest


def read_manifest(path: str | Path) -> FinancialSeedManifest:
    manifest_path = Path(path)
    tickers = []
    for raw_line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        tickers.append(line)
    ordered = tuple(sorted(set(tickers)))
    if not ordered:
        raise ValueError("financial seed manifest has no tickers")
    payload = ("\n".join(ordered) + "\n").encode()
    return FinancialSeedManifest(
        path=str(manifest_path.resolve()),
        tickers=ordered,
        sha256=hashlib.sha256(payload).hexdigest(),
    )

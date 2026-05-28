from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import app


def main() -> None:
    os.environ.setdefault('APP_WARMUP_ON_STARTUP', '0')
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    app.run(host='127.0.0.1', port=port, debug=False, use_reloader=False)


if __name__ == '__main__':
    main()

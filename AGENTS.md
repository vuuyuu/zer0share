# IPO-Score zer0share fork rules

- Preserve upstream zer0share behavior unless explicitly required.
- Keep IPO-Score changes isolated and minimal.
- Do not modify unrelated ETF/futures/options modules.
- Do not implement strategy scoring in this repository.
- This repository is data infrastructure only.
- IPO-Score initial market history starts at 2010-01-01.
- Subsequent updates are manually triggered incremental syncs.
- PIT-related financial fields must not be discarded.
- Run `uv run pytest` after each implementation step.
- Do not commit automatically unless explicitly requested.

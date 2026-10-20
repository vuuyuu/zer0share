# macOS 本机交易日定时同步

使用两个当前用户的 LaunchAgent，每天检查本地 SSE 交易日历：

| 任务 | 时间（本机时区应为 Asia/Shanghai） | 交易日执行命令 |
|---|---|---|
| `com.zer0share.sync-stock` | 17:00 | `uv run python main.py sync --stock` |
| `com.zer0share.sync-finance` | 21:00 | `uv run python main.py sync --finance` |

命令工作目录为安装时的 zer0share 目录。非交易日直接跳过；本地日历缺失、未覆盖当天或记录无效时明确报错，不擅自联网判断，也不将未知日期当作交易日。日历文件为 `data/stock/trade_cal/exchange=SSE/data.parquet`，实际 data 路径读取 `config/settings.toml`。

## 安装和验证

```bash
# 准备项目环境、股票列表和覆盖当前日期的交易日历
uv sync
uv run python main.py sync --table trade_cal
uv run python main.py sync --table basic

# 安装两个任务；安装本身不会立即下载
uv run python scripts/install_macos_sync.py

# 只检查日历及最终命令，不启动下载
uv run python -m zer0share.macos_sync stock --check-only
uv run python -m zer0share.macos_sync finance --check-only
```

安装文件位于 `~/Library/LaunchAgents/com.zer0share.sync-stock.plist` 和 `~/Library/LaunchAgents/com.zer0share.sync-finance.plist`。配置使用绝对解释器和 uv 路径，不依赖交互终端的 PATH；项目移动或重建环境后应卸载再安装。

```bash
launchctl print gui/$(id -u)/com.zer0share.sync-stock
launchctl print gui/$(id -u)/com.zer0share.sync-finance
```

此模式与 `main.py scheduler start` 二选一；LaunchAgents 不读取 `[scheduler]` 的逐表时间配置，不要同时启动旧调度器，以免重复同步。首次历史回填宜先手动完成。

## 等待与日志

- 两个任务共享 `logs/launchd/sync.lock`，21:00 若股票同步未结束，财务任务等待。
- 若 DuckDB 正被已经启动的手动命令占用，也会等待。检查与启动之间存在很短的间隙，运行定时任务期间仍应避免再启动独立同步进程。
- 每 30 秒检查一次，最多等待 12 小时；等待跨日则跳过旧任务，错误和超时会写日志。
- 同步完成或失败后释放互斥锁，后续任务可以继续；同步命令退出码保留在 LaunchAgent 中。
- 股票日志：`logs/launchd/stock.log`；财务日志：`logs/launchd/finance.log`；原有同步详细日志仍写入 `settings.toml` 指定的位置。
- 不修改系统休眠或唤醒设置。用户登录且系统唤醒时才能执行；休眠期间错过的日历事件由 launchd 在唤醒后合并触发，按当时的本地交易日历判断，不逐日重放。关机、注销期间不保证补跑，可手动执行同步补齐。
- 本地交易日历需要定期更新，尤其跨年前应确认覆盖下一年。任务按要求只读本地日历，不自动修复缺失日历。

## 卸载

```bash
uv run python scripts/install_macos_sync.py --uninstall
```

卸载只移除这两个任务，保留数据和日志。如任务正在运行，卸载会停止该任务及其同步子进程。

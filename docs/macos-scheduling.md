# macOS 本机交易日定时同步

使用两个当前用户的 LaunchAgent，每天检查本地 SSE 交易日历（股票判断当天，财务判断前一个自然日）：

| 任务 | 时间（本机时区应为 Asia/Shanghai） | 交易日执行命令 |
|---|---|---|
| `com.zer0share.sync-stock` | 17:00 | `uv run python main.py sync --stock` |
| `com.zer0share.sync-finance` | 02:00 | `uv run python main.py sync --finance --end-date <前一天YYYYMMDD>` |

命令工作目录为安装时的 zer0share 目录。非交易日直接跳过；本地日历缺失、未覆盖判断日期或记录无效时明确报错，不擅自联网判断，也不将未知日期当作交易日。日历文件为 `data/stock/trade_cal/exchange=SSE/data.parquet`，实际 data 路径读取 `config/settings.toml`。

财务例如周六凌晨 02:00 判断周五是否为交易日；周一凌晨判断周日，因此跳过。数据截止日期固定为前一天，传入 `--end-date`，起始日期仍按每只股票的同步进度自动计算。`sync_meta.last_date` 记录数据覆盖截止日期；`updated_at` 沿用原有实际写入时间语义，Parquet 文件修改时间和日志时间保留实际执行时间，不回拨系统日期。这样不会将凌晨当天提前标为已完成，也不会因任务运行跨日而改变本次截止日期。现有进度不自动回退。

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

- 两个任务共享 `logs/launchd/sync.lock`，02:00 若股票同步未结束，财务任务等待。
- 若 DuckDB 正被已经启动的手动命令占用，也会等待。检查与启动之间存在很短的间隙，运行定时任务期间仍应避免再启动独立同步进程。
- 每 30 秒检查一次，最多等待 12 小时；等待跨过实际执行日则跳过旧任务，错误和超时会写日志。
- 同步完成或失败后释放互斥锁，后续任务可以继续；同步命令退出码保留在 LaunchAgent 中。
- 股票日志：`logs/launchd/stock.log`；财务日志：`logs/launchd/finance.log`；原有同步详细日志仍写入 `settings.toml` 指定的位置。
- 不修改系统休眠或唤醒设置。用户登录且系统唤醒时才能执行；休眠期间错过的日历事件由 launchd 在唤醒后合并触发，按实际执行日期计算判断日期（股票当天、财务前一天），再查询本地交易日历，不逐日重放。关机、注销期间不保证补跑，可手动执行同步补齐。
- 本地交易日历需要定期更新，尤其跨年前应确认覆盖下一年。任务按要求只读本地日历，不自动修复缺失日历。

## 卸载

```bash
uv run python scripts/install_macos_sync.py --uninstall
```

卸载只移除这两个任务，保留数据和日志。如任务正在运行，卸载会停止该任务及其同步子进程。

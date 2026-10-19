# 财务专题

财务模块只同步 Tushare 当前源数据，不计算财务指标或策略评分，不进行 PIT 重建或历史版本归档。保留来源的公告日期、报告期、报表类型、更新标识、原始数值和单位，显式请求文档中的可选字段。

## 同步命令

先获取包含上市、退市股票的基础列表。财务同步不依赖交易日历；更新股票范围时再次运行 `basic`。

```bash
uv run python main.py sync --table basic
uv run python main.py sync --finance
```

专题包含以下 10 张表，也可单独同步：

```bash
uv run python main.py sync --table income           # 利润表
uv run python main.py sync --table balancesheet     # 资产负债表
uv run python main.py sync --table cashflow         # 现金流量表
uv run python main.py sync --table dividend         # 分红送股
uv run python main.py sync --table forecast         # 业绩预告
uv run python main.py sync --table express          # 业绩快报
uv run python main.py sync --table fina_indicator   # 财务指标
uv run python main.py sync --table fina_audit       # 财务审计意见
uv run python main.py sync --table fina_mainbz      # 主营业务构成
uv run python main.py sync --table disclosure_date  # 财报披露日期
```

`sync --all` 包含财务专题；`sync --stock` 保持原有范围。没有自动添加财务定时任务，后续手动执行 `sync --finance` 即可。

## 历史范围与刷新

- 每只股票首次同步默认从 **20000101** 至今天；各表实际历史覆盖以来源为准。
- 已完成首次同步的股票，每次刷新**上一年 1 月 1 日至今天**。2026 年运行刷新 20250101 至今，2027 年运行刷新 20260101 至今；同一天重复执行也会重新获取。
- 新股票、未完成首次下载的股票、丢失本地分区的股票仍补全首次历史。中断多年后再次运行，会补齐上次完成日期之后的缺口。
- 窗口内以本次完整源结果替换，包含修改和删除，窗口外保留。不另存旧版本、不按股票加报告期擅自去重，来源返回的不同类型记录均保留。
- 每只股票落盘后保存进度；获取或写入失败不推进该股票进度。下次执行时，已完成股票刷新近期范围，未完成股票继续补历史。

指定范围重拉更早数据：

```bash
uv run python main.py sync --table income --start-date 20100101 --end-date 20151231
uv run python main.py sync --finance --start-date 20000101 --end-date 20201231
```

显式指定较窄范围不代表首次历史完整；下一次无日期参数同步时，尚未完成初始化的股票仍从 20000101 补历史。

## 接口适配

使用普通逐股票接口，官方当前文档的这 10 个接口均列出至少 2000 积分。没有自动切换 VIP 全市场接口；全市场首次同步仍可能耗时较长。

日期适配是内部固定规则，无需额外配置：

| 表 | 同步及本地查询的日期范围字段 | 下载方式 |
|---|---|---|
| income / balancesheet / cashflow | ann_date | 传入 start_date / end_date |
| forecast / express / fina_audit | ann_date | 传入 start_date / end_date |
| fina_indicator / fina_mainbz | end_date（报告期） | 传入 start_date / end_date |
| dividend / disclosure_date | end_date（分红年度 / 报告期） | 按股票获取后筛选范围 |

`dividend` 和 `disclosure_date` 没有通用起止日期参数，不向它们传入不支持的参数。`fina_mainbz` 分别获取 P、D、I、S 分类，保留来源的 `bz_code`，不合并同名的不同分类。

每次实际请求后等待 1.5 秒，包括拆分后的请求和失败请求；网络连接错误有限重试，权限或业务错误直接报告。没有使用未文档化的 `offset` 分页：区间请求达到条数上限时拆成两个不重叠日期窗口。无法继续拆分或不支持区间的接口达到上限时，明确报错并保留旧数据，不接受可能截断的结果。

窗口外历史不自动核对；若来源修改窗口外数据，手动指定范围重拉。以报告期筛选的表，近期公告但报告期位于窗口外的记录不在自动刷新范围内。

## 存储和查询

每只股票一个当前文件：`data/finance/<表名>/ts_code=000001.SZ/data.parquet`。临时文件写完后原子替换，DuckDB 元数据库记录整表同步状态及各股票已完成的历史进度。仍应避免多个 shell 同时写同一 `db/meta.duckdb`。

```python
from zer0share import pro_api

pro = pro_api()
income = pro.income(ts_code="000001.SZ", start_date="20250101", end_date="20261231")
balance = pro.balancesheet(ts_code="000001.SZ", period="20251231")
cashflow = pro.cashflow(ts_code="000001.SZ", period="20251231")
dividends = pro.dividend(ts_code="000001.SZ", start_date="20250101", end_date="20261231")
indicators = pro.fina_indicator(period="20251231", fields="ts_code,ann_date,end_date,roe,eps")
business = pro.fina_mainbz(ts_code="000001.SZ", type="P", period="20251231")
disclosures = pro.disclosure_date(ts_code="000001.SZ", period="20251231")
audit = pro.query("fina_audit", ts_code="000001.SZ", period="20251231")
```

10 张表均支持 `pro.<表名>(...)` 和 `pro.query("<表名>", ...)`。支持股票代码（逗号分隔或列表）、日期范围、`period`（精确报告期）、`ann_date`、`fields`、`limit`、`offset`，以及来源字段等值筛选。`start_date` / `end_date` 使用上表固定日期字段，精确报告期统一使用 `period`。查询只读本地，不访问 Tushare。

## 官方文档

- [财务目录](https://tushare.pro/document/2?doc_id=16)
- [利润表](https://tushare.pro/document/2?doc_id=33)、[资产负债表](https://tushare.pro/document/2?doc_id=36)、[现金流量表](https://tushare.pro/document/2?doc_id=44)
- [分红送股](https://tushare.pro/document/2?doc_id=103)、[业绩预告](https://tushare.pro/document/2?doc_id=45)、[业绩快报](https://tushare.pro/document/2?doc_id=46)
- [财务指标](https://tushare.pro/document/2?doc_id=79)、[审计意见](https://tushare.pro/document/2?doc_id=80)、[主营业务构成](https://tushare.pro/document/2?doc_id=81)、[披露日期](https://tushare.pro/document/2?doc_id=162)

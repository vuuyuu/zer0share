import datetime as dt
from pathlib import Path

import click

from zer0share.config import load_config
from zer0share.logging import init_logger
from zer0share.notifier import build_notifier
from zer0share.pipeline import Pipeline
from zer0share.profiles import IPO_SCORE_DATED_TABLES, IPO_SCORE_INIT_START, IPO_SCORE_TABLES
from zer0share.quality.models import QualityRunOptions
from zer0share.quality.reporter import QualityReporter, format_summary
from zer0share.quality.runner import QualityRunner
from zer0share.quality.targets import select_targets
from zer0share.seed import FINANCIAL_TABLES, FinancialSeedRunner, FinancialSeedState, read_manifest
from zer0share.sources import DataSources, RiceQuantFetcher, TushareFetcher
from zer0share.storage import MetaStore
from zer0share.universe import build_universes, build_universes_range


def _validate_date(ctx, param, value):
    if value is None:
        return None
    try:
        dt.datetime.strptime(value, "%Y%m%d")
        return value
    except ValueError:
        raise click.BadParameter("格式应为 YYYYMMDD，例如 20240102")


def _parse_date(s: str):
    return dt.datetime.strptime(s, "%Y%m%d").date()


def _make_pipeline(config_path: str = "config/settings.toml") -> Pipeline:
    cfg = load_config(Path(config_path))
    init_logger(cfg.log_path)
    sources = DataSources(
        tushare=TushareFetcher(cfg.tushare_token),
        ricequant=(
            RiceQuantFetcher(
                username=cfg.ricequant.username,
                password=cfg.ricequant.password,
                license_key=cfg.ricequant.license_key,
            )
            if cfg.ricequant.enabled
            else None
        ),
    )
    notifier = build_notifier(cfg.notifier)
    return Pipeline(cfg, sources, notifier)


@click.group()
def cli():
    pass


FUTURES_TABLES = [
    "fut_basic",
    "fut_daily",
    "fut_holding",
    "fut_wsr",
    "fut_settle",
    "fut_mapping",
    "ft_limit",
    "fut_weekly",
    "fut_monthly",
    "fut_index_daily",
    "fut_weekly_detail",
]

OPTIONS_TABLES = [
    "opt_basic",
    "opt_daily",
]

ETF_TABLES = [
    "fund_daily",
    "fund_adj",
    "etf_share_size",
    "etf_sh_cons",
    "etf_basic",
    "etf_index",
]

STOCK_TABLES = [
    "basic",
    "trade_cal",
    "daily_kline",
    "adj_factor",
    "daily_basic",
    "stock_st",
    "suspend_d",
    "stk_limit",
    "index_daily",
    "index_weight",
    "idx_anns",
    "industry",
    "sw_daily",
    "ci_member",
]

RICEQUANT_TABLES = [
    "ricequant_basic",
    "ricequant_stock_minute",
    "ricequant_etf_basic",
    "ricequant_etf_minute",
]

SYNC_TABLES = [
    "fina_audit",
    "fina_indicator",
    "dividend",
    "income",
    "balancesheet",
    "cashflow",
    *STOCK_TABLES,
    *FUTURES_TABLES,
    *OPTIONS_TABLES,
    *ETF_TABLES,
    *RICEQUANT_TABLES,
]


@cli.command()
@click.option(
    "--table",
    type=click.Choice(SYNC_TABLES),
    default=None,
)
@click.option("--all", "sync_all", is_flag=True, default=False)
@click.option("--stock", "sync_stock", is_flag=True, default=False)
@click.option("--futures", "sync_futures", is_flag=True, default=False)
@click.option("--options", "sync_options", is_flag=True, default=False)
@click.option("--etf", "sync_etf", is_flag=True, default=False)
@click.option("--ricequant", "sync_ricequant", is_flag=True, default=False)
@click.option("--ipo-score", "sync_ipo_score", is_flag=True, default=False)
@click.option("--init", "init", is_flag=True, default=False)
@click.option("--start-date", default=None, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date)
def sync(
    table: str | None,
    sync_all: bool,
    sync_stock: bool,
    sync_futures: bool,
    sync_options: bool,
    sync_etf: bool,
    sync_ricequant: bool,
    sync_ipo_score: bool,
    init: bool,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """同步数据。"""
    selectors = (table is not None, sync_all, sync_stock, sync_futures,
                 sync_options, sync_etf, sync_ricequant, sync_ipo_score)
    if sum(selectors) > 1:
        raise click.UsageError(
            "--table, --all, --stock, --etf, --futures, --options, --ricequant, "
            "--ipo-score are mutually exclusive"
        )
    if init and not sync_ipo_score:
        raise click.UsageError("--init requires --ipo-score")

    effective_start = IPO_SCORE_INIT_START if init else start_date
    if end_date is not None and effective_start is None:
        raise click.UsageError("--end-date requires --start-date")
    if effective_start is not None and end_date is not None and end_date < effective_start:
        raise click.UsageError("--end-date must be on or after --start-date")

    with _make_pipeline() as pipeline:
        if table is not None and (start_date is not None or end_date is not None):
            job = pipeline.registry.get(table)
            if job is not None and not job.supports_date_range:
                raise click.UsageError("date range options are only supported for daily partitioned tables")

        if sync_ipo_score:
            for t in IPO_SCORE_TABLES:
                if t in {"fina_audit", "fina_indicator", "dividend", "income", "balancesheet", "cashflow"}:
                    # Initial history comes from this dataset's own first_date.
                    pipeline.run(t, start_date=None if init else start_date, end_date=end_date)
                    continue
                dated = t in IPO_SCORE_DATED_TABLES
                pipeline.run(
                    t,
                    start_date=effective_start if dated else None,
                    end_date=end_date if dated else None,
                )
        elif sync_all:
            pipeline.run_all(start_date=start_date, end_date=end_date)
        elif sync_stock:
            for t in STOCK_TABLES:
                pipeline.run(t, start_date=start_date, end_date=end_date)
        elif sync_futures:
            for t in FUTURES_TABLES:
                pipeline.run(t, start_date=start_date, end_date=end_date)
        elif sync_options:
            for t in OPTIONS_TABLES:
                pipeline.run(t, start_date=start_date, end_date=end_date)
        elif sync_etf:
            for t in ETF_TABLES:
                pipeline.run(t, start_date=start_date, end_date=end_date)
        elif sync_ricequant:
            for t in RICEQUANT_TABLES:
                pipeline.run(t, start_date=start_date, end_date=end_date)
        elif table is not None:
            run_kwargs = {"start_date": start_date, "end_date": end_date}
            if table == "industry":
                run_kwargs["allow_non_trading_day"] = True
            pipeline.run(table, **run_kwargs)
        else:
            raise click.UsageError("需要指定 --table、--stock、--futures、--options、--etf、--ricequant、--ipo-score 或 --all")


def _seed_state_path(config_path: str) -> Path:
    return load_config(Path(config_path)).data_dir / "ops" / "financial_seed_state.sqlite"


def _parse_seed_tables(value: str) -> tuple[str, ...]:
    tables = tuple(part.strip() for part in value.split(",") if part.strip())
    if not tables or set(tables) - set(FINANCIAL_TABLES):
        supported = ", ".join(FINANCIAL_TABLES)
        raise click.BadParameter(f"仅支持: {supported}")
    if len(set(tables)) != len(tables):
        raise click.BadParameter("tables must not contain duplicates")
    return tables


@cli.group("seed-financial")
def seed_financial() -> None:
    """管理金融数据全量回填任务。"""


@seed_financial.command("init")
@click.option("--manifest", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--tables", default=",".join(FINANCIAL_TABLES), show_default=True)
@click.option("--start-date", callback=_validate_date, required=True)
@click.option("--end-date", callback=_validate_date, required=True)
@click.option("--config", "config_path", default="config/settings.toml", show_default=True)
def seed_financial_init(
    manifest: Path,
    tables: str,
    start_date: str,
    end_date: str,
    config_path: str,
) -> None:
    """从显式 ticker manifest 创建可恢复的金融回填任务。"""
    if end_date < start_date:
        raise click.UsageError("--end-date must be on or after --start-date")
    parsed_tables = _parse_seed_tables(tables)
    parsed_manifest = read_manifest(manifest)
    with FinancialSeedState(_seed_state_path(config_path)) as state:
        run_id = state.create_run(parsed_manifest, parsed_tables, start_date, end_date)
    click.echo(
        f"run_id={run_id} tickers={len(parsed_manifest.tickers)} "
        f"tables={len(parsed_tables)} tasks={len(parsed_manifest.tickers) * len(parsed_tables)}"
    )
    if "dividend" in parsed_tables:
        click.echo("dividend retains its existing full-history fetch semantics; --start-date/--end-date are recorded but ignored by its API fetcher")


@seed_financial.command("run")
@click.option("--run-id", required=True)
@click.option("--manifest", type=click.Path(exists=True, dir_okay=False, path_type=Path), default=None)
@click.option("--table", "table_name", type=click.Choice(FINANCIAL_TABLES), default=None)
@click.option("--limit", type=click.IntRange(min=1), default=None)
@click.option("--retry-failed", is_flag=True, default=False)
@click.option("--config", "config_path", default="config/settings.toml", show_default=True)
def seed_financial_run(
    run_id: str,
    manifest: Path | None,
    table_name: str | None,
    limit: int | None,
    retry_failed: bool,
    config_path: str,
) -> None:
    """运行或恢复金融回填任务。"""
    with FinancialSeedState(_seed_state_path(config_path)) as state:
        if manifest is not None:
            state.verify_manifest(run_id, read_manifest(manifest))
        with _make_pipeline(config_path) as pipeline:
            jobs = {table: pipeline.registry[table] for table in FINANCIAL_TABLES}
            result = FinancialSeedRunner(state, jobs).run(
                run_id,
                table_name=table_name,
                limit=limit,
                retry_failed=retry_failed,
            )
    click.echo(
        f"attempted={result.attempted} success={result.success} "
        f"valid_empty={result.valid_empty} failed={result.failed}"
    )


@seed_financial.command("status")
@click.option("--run-id", required=True)
@click.option("--show-failed", is_flag=True, default=False)
@click.option("--config", "config_path", default="config/settings.toml", show_default=True)
def seed_financial_status(run_id: str, show_failed: bool, config_path: str) -> None:
    """查看金融回填任务状态。"""
    with FinancialSeedState(_seed_state_path(config_path)) as state:
        summary = state.summary(run_id)
        metadata = state.run_metadata(run_id)
        click.echo(f"run_id={run_id} tickers={metadata['ticker_count']} tables={metadata['tables_csv']}")
        for status, count in summary["total"].items():
            click.echo(f"{status}={count}")
        for table, counts in summary["by_table"].items():
            rendered = " ".join(f"{status}={count}" for status, count in counts.items())
            click.echo(f"{table}: {rendered}")
        if show_failed:
            for task in state.failed_tasks(run_id):
                click.echo(
                    f"FAILED {task['table_name']} {task['ticker']} "
                    f"{task['error_type']}: {task['error_message'] or ''}"
                )


@cli.group()
def quality():
    """数据质检。"""


@quality.command("check")
@click.option("--table", "table_name", default=None)
@click.option(
    "--market",
    type=click.Choice(["stock", "index", "etf", "futures", "options"]),
    default=None,
)
@click.option("--all", "all_targets", is_flag=True, default=False)
@click.option(
    "--mode",
    type=click.Choice(["full", "daily"]),
    default="daily",
    show_default=True,
)
@click.option("--start-date", default=None, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date)
@click.option("--date", "single_date", default=None, callback=_validate_date)
def quality_check(
    table_name: str | None,
    market: str | None,
    all_targets: bool,
    mode: str,
    start_date: str | None,
    end_date: str | None,
    single_date: str | None,
) -> None:
    """检查本地 Parquet 数据质量。"""
    if mode == "full" and (start_date is None or end_date is None):
        raise click.UsageError("full mode requires --start-date and --end-date")
    if mode == "daily" and (start_date is not None or end_date is not None):
        raise click.UsageError("daily mode uses --date, not --start-date/--end-date")
    if mode == "full" and single_date is not None:
        raise click.UsageError("full mode does not use --date")

    try:
        targets = select_targets(table=table_name, market=market, all_targets=all_targets)
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc

    cfg = load_config(Path("config/settings.toml"))
    options = QualityRunOptions(
        mode=mode,
        tables=tuple(target.table for target in targets),
        start_date=start_date,
        end_date=end_date,
        date=single_date,
    )
    report = QualityRunner(cfg.data_dir).run(options)
    QualityReporter(Path("reports") / "quality").write(report)
    click.echo(format_summary(report))
    if report.fail_count > 0:
        raise click.exceptions.Exit(1)


@cli.command("build-universe")
@click.option("--date", "trade_date", default=None, callback=_validate_date)
@click.option("--start-date", default=None, callback=_validate_date)
@click.option("--end-date", default=None, callback=_validate_date)
def build_universe_cmd(
    trade_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """构建股票池。"""
    if trade_date is not None and (start_date is not None or end_date is not None):
        raise click.UsageError("--date cannot be used with --start-date or --end-date")

    cfg = load_config(Path("config/settings.toml"))
    init_logger(cfg.log_path)
    if trade_date is not None:
        counts = build_universes(cfg.data_dir, _parse_date(trade_date))
        for name, count in counts.items():
            click.echo(f"{name}: {count}")
        return

    summary = build_universes_range(
        cfg.data_dir,
        start_date=_parse_date(start_date) if start_date is not None else None,
        end_date=_parse_date(end_date) if end_date is not None else None,
    )
    click.echo(
        f"range: {summary['start_date']} ~ {summary['end_date']}, "
        f"trading_days: {summary['trading_days']}, "
        f"built: {summary['built_days']}, skipped: {summary['skipped_days']}"
    )
    for name, count in summary["counts"].items():
        click.echo(f"{name}: {count}")


@cli.command()
def status() -> None:
    """显示各表最后更新时间。"""
    cfg = load_config(Path("config/settings.toml"))
    with MetaStore(cfg.db_path) as store:
        for table in SYNC_TABLES:
            last = store.get_last_date(table)
            click.echo(f"{table}: {last or '从未同步'}")


@cli.command("scheduler")
@click.argument("action", type=click.Choice(["start"]))
def scheduler_cmd(action: str) -> None:
    """启动定时调度。"""
    from zer0share.scheduler import start_scheduler

    start_scheduler()

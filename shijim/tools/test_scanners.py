import logging
import os

import pandas as pd
import shioaji as sj
from shioaji.constant import ScannerType


def run_scanner(api: sj.Shioaji, scanner_type: ScannerType, count: int, ascending: bool, date: str) -> None:
    logging.info("Running scanners: type=%s, count=%s, ascending=%s, date=%s", scanner_type, count, ascending, date)
    scanners = api.scanners(scanner_type=scanner_type, count=count, ascending=ascending, date=date)
    logging.info("Got %s records from scanners().", len(scanners))
    if not scanners:
        return
    # Basic assertions
    df = pd.DataFrame(s.__dict__ for s in scanners)
    assert not df.empty
    # keep only a few columns to print
    cols = ["date", "code", "name", "ts", "open", "high", "low", "close", "rank_value"]
    cols = [c for c in cols if c in df.columns]
    logging.info("DataFrame head:\n%s", df[cols].head())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.info("Logging in with token ...")
    api = sj.Shioaji()
    api.login(api_key=os.environ["SHIOAJI_API_KEY"], secret_key=os.environ["SHIOAJI_SECRET_KEY"])
    logging.info("Login done.")
    logging.info("Accounts: %s", api.list_accounts())

    date = os.getenv("UNIVERSE_SCANNER_DATE", "2025-11-21")
    count = int(os.getenv("UNIVERSE_SCANNER_COUNT", "5"))
    ascending = os.getenv("UNIVERSE_SCANNER_ASCENDING", "false").lower() in ("1", "true", "yes")

    # Single case
    logging.info("\n################################################################################")
    logging.info("[SINGLE CASE] ChangePercentRank, count=%s, ascending=%s", count, ascending)
    run_scanner(api, ScannerType.ChangePercentRank, count=count, ascending=ascending, date=date)

    # Batch cases
    logging.info("\n################################################################################")
    logging.info("[BATCH CASES] Test all supported scanner types")
    for stype in [
        ScannerType.ChangePercentRank,
        ScannerType.ChangePriceRank,
        ScannerType.DayRangeRank,
        ScannerType.VolumeRank,
        ScannerType.AmountRank,
    ]:
        logging.info("\n================================================================================")
        logging.info("[CASE] Testing %s", stype)
        run_scanner(api, stype, count=count, ascending=ascending, date=date)

    logging.info("\n[INFO] Logout.")
    api.logout()


if __name__ == "__main__":
    main()

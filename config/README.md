# Config Profiles

This directory stores reusable run profiles for the Secure Scraper CLI. The default entry point
(`config/run_config.toml`) remains a light two-week lookahead, but recurring scenarios now live
under `config/routines/` so we can keep single-purpose configs grouped by intent.

```
config/
├── README.md                 # this file
├── run_config.toml           # default profile used when --config isn't supplied
└── routines/
    └── global/               # global sweeps that hit every catalog destination (destinations = ["*"])
        ├── today.toml        # single-day snapshot
        ├── next-7-days.toml  # rolling week horizon
        └── next-14-days.toml # two-week horizon
```

Use any profile by pointing the runner at it:

```bash
PYTHONPATH=src python scripts/run_scraper.py --config config/routines/global/next-7-days.toml
```

Each routine sets `destinations = ["*"]` and enables SQLite storage so longer runs are resumable. The
new `sweep_priority` toggle (see `scripts/run_scraper.py`) lets us choose whether the scraper walks
**date-first** (default legacy behavior) or **destination-first** (finish every date for one destination
before moving to the next). Global routines default to the destination-first priority so we avoid
reloading the full catalog for each new check-in.

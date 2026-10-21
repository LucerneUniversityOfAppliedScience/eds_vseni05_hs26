import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="15 – Holidays and traffic")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 15 – Holidays and traffic: do school holidays fill the airports?

    Leisure travel follows the calendar: school holidays, long weekends, public holidays. But **whose** calendar?
    An aircraft from Zurich to Palma is full when Zurich has holidays on the way out – and when Germany has holidays,
    the German tourists fill the same island.

    Our own flight data covers only a few days (the class bucket starts on 19.10.2026) – far too short to see a
    holiday effect. So this notebook combines three sources:

    | source | what | licence |
    |---|---|---|
    | [OpenHolidays API](https://www.openholidaysapi.org) | school and public holidays per country and region – 36 countries (CH, DE, FR, AT, NL, ES, IT, ... – **not** GB; `GET /Countries`, 20.10.2026) | [ODbL](https://github.com/openpotato/openholidaysapi.data) |
    | [Nager.Date](https://date.nager.at) | public holidays, also for GB | code MIT; API free for private and non-profit use ([terms](https://nagerholidays.com/legal/termsofservice)) |
    | [EUROCONTROL](https://www.eurocontrol.int/performance/data/download/) | **daily** IFR movements per airport, 2026 until 30 September (as of 20.10.2026) | free for non-commercial use, source: EUROCONTROL ([disclaimer](https://ansperformance.eu/about/disclaimer/)) |

    No keys needed. Every answer is cached in `cache/`, so the class does not hit the APIs 25 times.
    """)
    return


@app.cell
def _(mo):
    import sys
    from urllib.parse import urlencode

    import pandas as pd
    import plotly.express as px

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    def openholidays(kind, country, subdivision=None, start="2026-01-01", end="2026-12-31"):
        """Raw answer of the OpenHolidays API (a list of dicts), cached for a week. kind: School | Public."""
        import json

        _q = {"countryIsoCode": country, "languageIsoCode": "EN", "validFrom": start, "validTo": end}
        if subdivision:
            _q["subdivisionCode"] = subdivision
        _url = f"https://openholidaysapi.org/{kind}Holidays?" + urlencode(_q)
        _name = f"openholidays_{kind.lower()}_{subdivision or country}_{start}_{end}.json"
        return json.loads(fl.cached(_url, _name, 7).read_text())

    with mo.status.spinner(title="downloading the EUROCONTROL airport traffic 2026 (~6 MB, once a week) ..."):
        eurocontrol_csv = fl.cached("https://www.eurocontrol.int/performance/data/download/csv/airport_traffic_2026.csv",
                                     "eurocontrol_airport_traffic_2026.csv", 7)

    con = fl.snapshot()
    fl.ensure(con, "flight_routes")                     # tables the app computes – here if you only ran collector.py
    con.execute("CREATE SCHEMA IF NOT EXISTS holidays")
    span = con.execute("SELECT MIN(snapshot_time), MAX(snapshot_time) FROM silver.flights "
                       "WHERE region = 'switzerland'").fetchone()
    mo.md(f"your flight data: **{span[0]:%d.%m.%Y} – {span[1]:%d.%m.%Y}** · EUROCONTROL file: `{eurocontrol_csv.name}`")
    return check, con, eurocontrol_csv, fl, openholidays, pd, px, span


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 15.1 – From holiday periods to holiday days

    The API returns **periods**. One school holiday of Zurich looks like this (shortened):

    ```json
    {"startDate": "2026-10-05", "endDate": "2026-10-17", "type": "School",
     "name": [{"language": "EN", "text": "Autumn holidays"}],
     "subdivisions": [{"code": "CH-ZH", "shortName": "ZH"}],
     "groups": [{"code": "CH-ZH-VS", "shortName": "ZH-VS"}]}
    ```

    (Official dates: the canton lists the autumn holidays 2026 as Monday 05.10. to Saturday 17.10.26 for its
    Mittel- and Berufsfachschulen; the Volksschule follows weeks 41–42, set by each municipality –
    [zh.ch, Schulferien](https://www.zh.ch/de/bildung/bildungssystem/schulferien.html). The ski holidays
    in February are set by each municipality – the API has none for ZH.)

    To join with daily traffic we need **one row per day and region**. Write
    `holiday_days(records, country, kind)` that returns a DataFrame with the columns
    `country`, `subdivision`, `day` (a date), `kind`, `name`:

    - one row per day from `startDate` to `endDate` (**both included**) and per subdivision;
    - subdivision codes cut to the first level: `NL-ZH-RT` → `NL-ZH` (the API lists Dutch school holidays per
      province and municipality – `NL-ZH-RT` is Rotterdam in South Holland);
    - no `subdivisions` (a **nationwide** holiday) → `subdivision` = the country code, e.g. `CH`;
    - **no duplicates** per (`country`, `subdivision`, `day`, `kind`) – Swiss entries come once per school group
      (e.g. `ZH-VS` and `ZH-MS`, the same dates).

    /// details | Hint
    `pd.date_range(start, end)` includes both ends; `"-".join(code.split("-")[:2])` cuts the code;
    `df.drop_duplicates([...])` at the end. `r.get("subdivisions") or []` handles a missing key.
    ///
    """)
    return


@app.cell
def _(pd):
    # >>> TODO 15.1: holiday_days(records, country, kind) -> one row per day and subdivision, without duplicates
    # ?? def holiday_days(records, country, kind):
    # ??     return pd.DataFrame(columns=["country", "subdivision", "day", "kind", "name"])
    def holiday_days(records, country, kind):
        rows = []
        for r in records:
            subs = sorted({"-".join(s["code"].split("-")[:2]) for s in r.get("subdivisions") or []}) or [country]
            name = r["name"][0]["text"] if r.get("name") else None
            for day in pd.date_range(r["startDate"], r["endDate"]).date:
                rows += [(country, sub, day, kind, name) for sub in subs]
        return (pd.DataFrame(rows, columns=["country", "subdivision", "day", "kind", "name"])
                .drop_duplicates(["country", "subdivision", "day", "kind"]).reset_index(drop=True))
    # <<< TODO 15.1
    return (holiday_days,)


@app.cell
def _(con, holiday_days, openholidays, pd):
    _zh = pd.concat([holiday_days(openholidays("School", "CH", "CH-ZH"), "CH", "school"),
                     holiday_days(openholidays("Public", "CH", "CH-ZH"), "CH", "public")], ignore_index=True)
    con.execute("CREATE OR REPLACE TABLE holidays.days (country VARCHAR, subdivision VARCHAR, day DATE, kind VARCHAR, "
                "name VARCHAR)")
    con.register("zh_holidays", _zh)                     # marimo renames _private names: register explicitly
    con.execute("INSERT INTO holidays.days SELECT country, subdivision, TRY_CAST(day AS DATE), kind, name "
                "FROM zh_holidays")
    con.unregister("zh_holidays")
    zh_days = con.execute("SELECT kind, name, MIN(day) AS first_day, MAX(day) AS last_day, COUNT(*) AS days "
                          "FROM holidays.days WHERE subdivision IN ('CH-ZH', 'CH') GROUP BY ALL ORDER BY first_day").df()
    zh_days
    return (zh_days,)


@app.cell(hide_code=True)
def _(check, con, zh_days):
    try:
        _dup = con.execute("SELECT COUNT(*) - COUNT(DISTINCT (country, subdivision, day, kind)) "
                           "FROM holidays.days").fetchone()[0]
        _autumn = con.execute("SELECT COUNT(*) FROM holidays.days WHERE subdivision = 'CH-ZH' AND kind = 'school' "
                              "AND day BETWEEN '2026-10-05' AND '2026-10-17'").fetchone()[0]
        _aug1 = con.execute("SELECT COUNT(*) FROM holidays.days WHERE subdivision = 'CH' AND kind = 'public' "
                            "AND day = '2026-08-01'").fetchone()[0]
        _ok = _dup == 0 and _autumn == 13 and _aug1 == 1
        _school = int(zh_days[zh_days.kind == "school"]["days"].sum())
        _msg = (f"Zurich: {_school} school holiday days (incl. weekends) and "
                f"{int((zh_days.kind == 'public').sum())} public holidays – autumn holidays 5.–17.10.2026 complete, "
                f"1 August nationwide, no duplicates" if _ok else "")
    except Exception:
        _ok, _msg, _dup, _autumn = False, "", "?", "?"
    check(_ok, _msg, f"Expected 13 rows for the Zurich autumn holidays 5.–17.10.2026 (got {_autumn}), 1 August as "
                     f"nationwide public holiday (subdivision 'CH') and no duplicates (got {_dup}).")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 15.2 – Zurich airport day by day, with holiday flags

    The EUROCONTROL file has one row per airport and day: `FLT_DATE`, `APT_ICAO`, `FLT_DEP_1`, `FLT_ARR_1`,
    `FLT_TOT_1` (departures + arrivals, IFR). Build `lszh_daily` for **LSZH** with the columns
    `day`, `weekday` (`dayname(...)`), `movements` (= `FLT_TOT_1`), `school_holiday` and `public_holiday` (booleans:
    is the day a holiday of Zurich – subdivision `CH-ZH` **or** nationwide `CH` – in `holidays.days`?).

    Careful with "public holiday": the canton of Zurich has **9** statutory holidays
    ([zh.ch, Feiertage](https://zh.ch/de/wirtschaft-arbeit/arbeitsbedingungen/arbeitsssicherheit-gesundheitsschutz/arbeits-ruhezeiten/feiertage.html)).
    The API also lists Berchtoldstag (2 Jan), the Federal Fast (a Sunday) and the city of Zurich's Sechseläuten and
    Knabenschiessen (`CH-ZH-ZH-ZH` – local, non-statutory; our cut to `CH-ZH` spreads them over the whole canton).

    The file path is `eurocontrol_csv` (setup cell): `read_csv('{eurocontrol_csv}')` – here the default type
    detection works (ISO dates).

    /// details | Hint
    ```sql
    SELECT e.FLT_DATE AS day, ...,
           EXISTS (SELECT 1 FROM holidays.days h
                   WHERE h.day = e.FLT_DATE AND h.kind = 'school' AND h.subdivision IN ('CH-ZH', 'CH')) AS school_holiday
    FROM read_csv('...') e WHERE e.APT_ICAO = 'LSZH'
    ```
    ///
    """)
    return


@app.cell
def _(con, eurocontrol_csv, mo, zh_days):
    zh_days                                              # rerun after the holiday table changed
    # >>> TODO 15.2: lszh_daily = LSZH movements per day 2026 with school_holiday / public_holiday flags of Zurich
    # ?? lszh_daily = mo.sql("SELECT DATE '2026-01-01' AS day, 'Thursday' AS weekday, 0 AS movements, FALSE AS school_holiday, FALSE AS public_holiday", engine=con)
    lszh_daily = mo.sql(
        f"""
        SELECT e.FLT_DATE AS day, dayname(e.FLT_DATE) AS weekday, e.FLT_TOT_1 AS movements,
               EXISTS (SELECT 1 FROM holidays.days h WHERE h.day = e.FLT_DATE AND h.kind = 'school'
                         AND h.subdivision IN ('CH-ZH', 'CH')) AS school_holiday,
               EXISTS (SELECT 1 FROM holidays.days h WHERE h.day = e.FLT_DATE AND h.kind = 'public'
                         AND h.subdivision IN ('CH-ZH', 'CH')) AS public_holiday
        FROM read_csv('{eurocontrol_csv}') e
        WHERE e.APT_ICAO = 'LSZH'
        ORDER BY day
        """,
        engine=con,
    )
    # <<< TODO 15.2
    return (lszh_daily,)


@app.cell(hide_code=True)
def _(check, lszh_daily, mo, pd, px):
    _cols = {"day", "weekday", "movements", "school_holiday", "public_holiday"}
    _out = []
    try:
        _d = lszh_daily.copy()
        _ok = (_cols <= set(_d.columns) and len(_d) > 200 and _d["school_holiday"].sum() > 30
               and _d["public_holiday"].sum() >= 5 and _d["movements"].min() > 0)
    except Exception:
        _ok = False
    if _ok:
        _d["day"] = pd.to_datetime(_d["day"])
        _work = _d[~_d.public_holiday]
        _h, _n = _work[_work.school_holiday].movements.mean(), _work[~_work.school_holiday].movements.mean()
        _p = _d[_d.public_holiday].movements.mean()
        _out.append(check(True, f"{len(_d)} days until {_d.day.max():%d.%m.%Y}: on average {_h:.0f} movements on "
                                f"Zurich school holiday days, {_n:.0f} on other days ({(_h / _n - 1) * 100:+.0f} %), "
                                f"{_p:.0f} on public holidays", ""))
        _d["calendar"] = _d.apply(lambda r: "public holiday" if r.public_holiday else
                                  ("school holiday ZH" if r.school_holiday else "normal day"), axis=1)
        _out.append(px.scatter(_d, x="day", y="movements", color="calendar", height=320,
                               color_discrete_map={"normal day": "#9aa0a6", "school holiday ZH": "#e8710a",
                                                   "public holiday": "#d93025"},
                               labels={"day": "", "movements": "LSZH movements per day (EUROCONTROL)"}))
        _out.append(mo.md("Careful: the summer holidays fall in the busiest months anyway (July–September had the "
                          "most movements per day at LSZH in 2026) – part of the difference is **season**, not "
                          "holidays. 15.4 looks closer."))
    else:
        _out.append(check(False, "", f"Expected one row per day for LSZH with {sorted(_cols)} – at least 30 school "
                                     "holiday days and 5 public holidays of Zurich in 2026 (TODO 15.1 done?)."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 15.3 – Where do the aircraft above us go – and is it holiday there?

    The cell below loads the **school holidays 2026** of the most frequent destination countries in your observed
    routes (`silver.flight_routes`, as in notebook 10) into `holidays.days` – with your function. GB is not in the
    OpenHolidays API (`GET /Countries`); for GB only the public holidays come from Nager.Date.

    Build `destinations_now`: per `destination_country` of the flights seen in the Swiss feed with a known route
    (different `icao24` + `callsign`) the number of `flights`, and on the **last day of your data** (`span[1]`):

    - `regions_on_holiday` – how many first-level regions of the country have school holidays (a nationwide row
      counts as all regions),
    - `regions` – how many regions the country has in `holidays.days` (all regions that have school holidays at
      some point in 2026),
    - `holiday_share` = `regions_on_holiday / regions`, **NULL when the country has no school data** for that day
      (not covered, e.g. GB, or the data ends earlier – Italy's 2026 school holidays in the API end on 16.06.2026,
      checked 20.10.2026). Unknown is not "no holiday".

    Most flights first.

    /// details | Hint
    Three CTEs: `seen` (distinct flights with route, as in 10.2), `cover` (per country from `holidays.days`
    where `kind = 'school'`: `COUNT(DISTINCT subdivision) FILTER (WHERE subdivision <> country)` and `MAX(day)`),
    `today` (per country on that day: regions on holiday, `bool_or(subdivision = country)` for nationwide).
    Then `CASE WHEN c.last_day >= DATE '...' THEN ... END`.
    ///
    """)
    return


@app.cell
def _(con, fl, holiday_days, openholidays, pd, zh_days):
    import json

    zh_days                                              # after the Zurich table
    _ohd = {"AD", "AL", "AT", "BE", "BG", "BR", "BY", "CH", "CZ", "DE", "EE", "ES", "FR", "HR", "HU", "IE", "IT",
            "LI", "LT", "LU", "LV", "MC", "MD", "MT", "MX", "NL", "PL", "PT", "RO", "RS", "SE", "SI", "SK", "SM",
            "VA", "ZA"}                                  # countries of the OpenHolidays API (GET /Countries, Oct 2026)
    _top = [c for (c,) in con.execute("""
        WITH seen AS (SELECT DISTINCT icao24, trim(callsign) AS callsign FROM silver.flights
                      WHERE region = 'switzerland' AND callsign IS NOT NULL)
        SELECT r.destination_country FROM seen JOIN silver.flight_routes r USING (callsign)
        WHERE r.destination_country IS NOT NULL GROUP BY 1 ORDER BY COUNT(*) DESC""").fetchall()]
    dest_countries = list(dict.fromkeys(["DE"] + [c for c in _top if c in _ohd and c != "CH"][:8]))
    _parts = [holiday_days(openholidays("School", _c), _c, "school") for _c in dest_countries]
    _parts += [holiday_days(openholidays("School", "CH"), "CH", "school")]        # all cantons, not only ZH
    _gb = json.loads(fl.cached("https://date.nager.at/api/v3/PublicHolidays/2026/GB", "nager_public_GB_2026.json",
                                7).read_text())
    _parts += [holiday_days([{"startDate": h["date"], "endDate": h["date"], "name": [{"text": h["name"]}],
                              "subdivisions": [{"code": c} for c in h.get("counties") or []]} for h in _gb],
                            "GB", "public")]
    _more = pd.concat(_parts, ignore_index=True)
    con.execute("DELETE FROM holidays.days WHERE kind = 'school' AND country <> 'CH' OR country = 'GB' "
                "OR (country = 'CH' AND kind = 'school' AND subdivision <> 'CH-ZH')")
    con.register("more_holidays", _more)
    con.execute("""INSERT INTO holidays.days SELECT country, subdivision, TRY_CAST(day AS DATE), kind, name
                   FROM more_holidays WHERE NOT (country = 'CH' AND subdivision = 'CH-ZH')""")
    con.unregister("more_holidays")
    holiday_countries = con.execute("""
        SELECT country, kind, COUNT(DISTINCT subdivision) AS regions, MIN(day) AS first_day, MAX(day) AS last_day
        FROM holidays.days GROUP BY ALL ORDER BY country, kind""").df()
    holiday_countries
    return dest_countries, holiday_countries


@app.cell
def _(con, holiday_countries, mo, span):
    holiday_countries                                    # rerun after the holidays were loaded
    # >>> TODO 15.3: destinations_now = flights per destination_country with regions_on_holiday, regions, holiday_share (NULL = unknown)
    # ?? destinations_now = mo.sql("SELECT 'XX' AS destination_country, 0 AS flights, 0 AS regions_on_holiday, 0 AS regions, 0.0 AS holiday_share", engine=con)
    destinations_now = mo.sql(
        f"""
        WITH seen AS (
            SELECT DISTINCT icao24, trim(callsign) AS callsign FROM silver.flights
            WHERE region = 'switzerland' AND callsign IS NOT NULL),
        flights AS (
            SELECT r.destination_country, COUNT(*) AS flights
            FROM seen s JOIN silver.flight_routes r USING (callsign)
            WHERE r.destination_country IS NOT NULL
            GROUP BY ALL),
        cover AS (
            SELECT country, COUNT(DISTINCT subdivision) FILTER (WHERE subdivision <> country) AS regions,
                   MAX(day) AS last_day
            FROM holidays.days WHERE kind = 'school' GROUP BY ALL),
        today AS (
            SELECT country, COUNT(DISTINCT subdivision) FILTER (WHERE subdivision <> country) AS on_holiday,
                   bool_or(subdivision = country) AS nationwide
            FROM holidays.days WHERE kind = 'school' AND day = DATE '{span[1]:%Y-%m-%d}' GROUP BY ALL)
        SELECT f.destination_country, f.flights,
               CASE WHEN c.last_day >= DATE '{span[1]:%Y-%m-%d}'
                    THEN CASE WHEN t.nationwide THEN c.regions ELSE COALESCE(t.on_holiday, 0) END END AS regions_on_holiday,
               c.regions,
               CASE WHEN c.last_day >= DATE '{span[1]:%Y-%m-%d}'
                    THEN ROUND(CASE WHEN t.nationwide THEN 1.0
                                    ELSE COALESCE(t.on_holiday, 0) / NULLIF(c.regions, 0) END, 2) END AS holiday_share
        FROM flights f
        LEFT JOIN cover c ON c.country = f.destination_country
        LEFT JOIN today t ON t.country = f.destination_country
        ORDER BY f.flights DESC
        """,
        engine=con,
    )
    # <<< TODO 15.3
    return (destinations_now,)


@app.cell(hide_code=True)
def _(check, dest_countries, destinations_now, mo, px, span):
    _cols = {"destination_country", "flights", "regions_on_holiday", "regions", "holiday_share"}
    _out = []
    try:
        _d = destinations_now
        _de = _d.set_index("destination_country")["holiday_share"].get("DE")
        _gb = _d.set_index("destination_country")["holiday_share"].get("GB", None)
        _ok = (_cols <= set(_d.columns) and len(_d) > 5 and _d.flights.is_monotonic_decreasing
               and _de is not None and _de == _de and 0 < _de <= 1
               and (_gb is None or _gb != _gb) and _d.holiday_share.dropna().between(0, 1).all())
    except Exception:
        _ok = False
    if _ok:
        _known = _d.dropna(subset=["holiday_share"])
        _w = (_known.flights * _known.holiday_share).sum() / _known.flights.sum() if len(_known) else 0
        _on = ", ".join(f"{r.destination_country} {r.holiday_share:.0%}" for r in _known.itertuples()
                        if r.holiday_share > 0) or "none"
        _out.append(check(True, f"{span[1]:%d.%m.%Y}: school holidays in {_on}; weighted by flights, "
                                f"{_w:.0%} of the destinations with known calendar are on holiday – "
                                f"{_d.holiday_share.isna().sum()} countries unknown (no data)", ""))
        _top = _d.head(15).fillna({"holiday_share": -0.1})
        _out.append(px.bar(_top, x="destination_country", y="flights", color="holiday_share", height=300,
                           color_continuous_scale="Oranges", range_color=[0, 1],
                           labels={"destination_country": "destination country", "flights": "flights seen",
                                   "holiday_share": "share of regions on school holiday (-0.1 = unknown)"}))
        _out.append(mo.md(f"Loaded school calendars: {', '.join(dest_countries)} + all Swiss cantons; GB only "
                          f"public holidays (Nager.Date)."))
    else:
        _out.append(check(False, "", f"Expected {sorted(_cols)} per destination country, most flights first – "
                                     "Germany with a share between 0 and 1 on the last day of your data, and NULL "
                                     "(not 0) for countries without school data such as GB."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 15.4 – Weekday or holiday: what matters more?

    Airports have a weekly rhythm: at LSZH, Jan–Sep 2026, Friday and Monday were the busiest days on average, Tuesday
    and Saturday the quietest (EUROCONTROL file, our calculation). Is that business travel – and does it change in
    the holidays? Compare per weekday:
    build `weekday_effect` from `lszh_daily` (without public holidays) with the columns `weekday`, `normal`
    (mean movements on days **without** Zurich school holidays), `holiday` (mean **with**), and
    `diff_pct` = `(holiday / normal − 1) · 100`, ordered **Monday to Sunday**.

    /// details | Hint
    pandas: `pivot_table(index="weekday", columns="school_holiday", values="movements", aggfunc="mean")`, then
    rename the columns `False` / `True`. The order: `.reindex(["Monday", ..., "Sunday"])`. Or SQL with
    `AVG(movements) FILTER (WHERE school_holiday)` and `ORDER BY isodow(day)`.
    ///
    """)
    return


@app.cell
def _(lszh_daily, pd):
    DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    # >>> TODO 15.4: weekday_effect = weekday, normal, holiday, diff_pct (Monday..Sunday, without public holidays)
    # ?? weekday_effect = pd.DataFrame({"weekday": DAYS, "normal": 0.0, "holiday": 0.0, "diff_pct": 0.0})
    _p = (lszh_daily[~lszh_daily["public_holiday"].astype(bool)]
          .pivot_table(index="weekday", columns="school_holiday", values="movements", aggfunc="mean")
          .rename(columns={False: "normal", True: "holiday"}).reindex(DAYS))
    weekday_effect = _p.assign(diff_pct=(_p["holiday"] / _p["normal"] - 1) * 100).round(1).reset_index()
    weekday_effect.columns.name = None
    # <<< TODO 15.4
    return DAYS, weekday_effect


@app.cell(hide_code=True)
def _(DAYS, check, lszh_daily, mo, px, weekday_effect):
    _out = []
    try:
        _w = weekday_effect
        _d = lszh_daily[~lszh_daily["public_holiday"].astype(bool)]
        _sat = _d[(_d.weekday == "Saturday") & (_d.school_holiday.astype(bool))].movements.mean()
        _ok = (list(_w["weekday"]) == DAYS and {"normal", "holiday", "diff_pct"} <= set(_w.columns)
               and abs(_w.set_index("weekday").loc["Saturday", "holiday"] - _sat) < 1
               and (_w.diff_pct - (_w.holiday / _w.normal - 1) * 100).abs().max() < 0.2)
    except Exception:
        _ok = False
    if _ok:
        _big = _w.loc[_w.diff_pct.idxmax()]
        _spread = _w.normal.max() - _w.normal.min()
        _out.append(check(True, f"weekday spread on normal days: {_spread:.0f} movements "
                                f"({_w.loc[_w.normal.idxmin(), 'weekday']} lowest, {_w.loc[_w.normal.idxmax(), 'weekday']} "
                                f"highest); the school holiday effect is largest on {_big.weekday} "
                                f"({_big.diff_pct:+.0f} %)", ""))
        _out.append(px.bar(_w.melt(id_vars="weekday", value_vars=["normal", "holiday"], var_name="days",
                                   value_name="movements"), x="weekday", y="movements", color="days",
                           barmode="group", height=300, labels={"movements": "mean LSZH movements per day"}))
    else:
        _out.append(check(False, "", "Expected 7 rows Monday..Sunday with normal, holiday (mean movements without / "
                                     "with Zurich school holidays, public holidays excluded) and diff_pct."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Where people fly to: Palma and the German holidays, London and the bank holidays

    The same EUROCONTROL file has the large European airports. In 2025, **33.7 %** of the tourists in Mallorca came
    from Germany (provisional, [AETIB from FRONTUR/IBESTAT](https://www.caib.es/sites/estadistiquesdelturisme/f/550936)).
    The chart puts the daily movements of **Palma de Mallorca (LEPA)** next to the share of German Länder on school
    holiday (from `holidays.days`). **London Heathrow (EGLL)**, number 4 in the 2026 file: do the GB public holidays
    (Nager.Date) show at all?
    """)
    return


@app.cell
def _(con, eurocontrol_csv, holiday_countries, mo, px):
    holiday_countries
    _lepa = con.execute(f"""
        WITH de AS (
            SELECT day, COUNT(DISTINCT subdivision) FILTER (WHERE subdivision <> 'DE') AS on_holiday
            FROM holidays.days WHERE country = 'DE' AND kind = 'school' GROUP BY ALL),
        n AS (SELECT COUNT(DISTINCT subdivision) AS regions FROM holidays.days
              WHERE country = 'DE' AND kind = 'school' AND subdivision <> 'DE')
        SELECT e.FLT_DATE AS day, e.FLT_TOT_1 AS movements,
               ROUND(COALESCE(de.on_holiday, 0) / NULLIF(n.regions, 0), 2) AS de_school_share
        FROM read_csv('{eurocontrol_csv}') e CROSS JOIN n LEFT JOIN de ON de.day = e.FLT_DATE
        WHERE e.APT_ICAO = 'LEPA' ORDER BY day""").df()
    _egll = con.execute(f"""
        SELECT AVG(e.FLT_TOT_1) FILTER (WHERE h.day IS NOT NULL) AS bank_holiday,
               AVG(e.FLT_TOT_1) FILTER (WHERE h.day IS NULL AND isodow(e.FLT_DATE) = 1) AS other_mondays
        FROM read_csv('{eurocontrol_csv}') e
        LEFT JOIN (SELECT DISTINCT day FROM holidays.days WHERE country = 'GB' AND subdivision IN ('GB', 'GB-ENG'))
             h ON h.day = e.FLT_DATE
        WHERE e.APT_ICAO = 'EGLL'""").fetchone()
    if len(_lepa) and _lepa.de_school_share.notna().any():
        _fig = px.line(_lepa, x="day", y="movements", height=320,
                       labels={"day": "", "movements": "LEPA movements per day"})
        _fig.add_bar(x=_lepa.day, y=_lepa.de_school_share * _lepa.movements.max(), name="German Länder on school "
                     "holiday (scaled)", marker_color="orange", opacity=0.35)
        _corr = _lepa.movements.corr(_lepa.de_school_share)
        _view = mo.vstack([_fig, mo.md(
            f"Correlation LEPA movements – German school holiday share: **{_corr:+.2f}** (season and holidays are "
            f"tangled: summer is both). Heathrow: **{_egll[0]:.0f}** movements on English bank holidays vs. "
            f"**{_egll[1]:.0f}** on other Mondays." if _egll[0] else
            f"Correlation LEPA movements – German school holiday share: **{_corr:+.2f}**.")])
    else:
        _view = mo.md("*No German school holidays in `holidays.days` yet – finish TODO 15.1.*")
    _view
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - The Zurich autumn holidays 2026 ended on **17 October** – two days before our flight data starts. Which
      return flights could still be in your data on 19.10? And which destinations are on holiday right now?
    - The summer holidays coincide with the high season. How could you separate the two? (compare the same weeks
      of 2025 – EUROCONTROL also has `airport_traffic_2025.csv` – or compare with an airport in a country whose
      holidays fall elsewhere)
    - A "school holiday share" counts regions, not people: Bremen and Bavaria weigh the same. What would be fairer?
    - Unknown is not zero: Italy's school calendar in the API ends in June 2026, GB has none. How did you keep them
      out of the averages?
    - Many leisure destinations are islands (Palma, the Greek islands) that no train reaches – but the passengers
      could choose the destination. What does that mean for reducing aviation CO₂?

    Data: OpenHolidays API (ODbL), Nager.Date (free for non-profit use), EUROCONTROL Aviation Intelligence Portal (source: EUROCONTROL,
    non-commercial use).

    **Next:** [16 – Contrails with pycontrails](16_pycontrails.py)
    """)
    return


if __name__ == "__main__":
    app.run()

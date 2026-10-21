# Ported from the Databricks recipe (src_geo/07_silver_restricted_airspace.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver reference: closed and avoided airspace, per FIR – a CURATED LIST IN CODE.
#
# Why in code? There is no free, machine-readable live source for "which airspace is closed to whom" that a Databricks
# Free Edition pipeline can reach: NOTAM APIs need accounts, EASA publishes its Conflict Zone Information Bulletins
# (CZIBs) as web pages / PDFs, and national bans are press releases. So this file is a SNAPSHOT that a human has to
# maintain – exactly like a real airline's "airspace risk register". Students: check LAST_REVIEWED, open the sources
# and update the list before you trust any number built on it. Every row carries its source, its validity and a
# confidence level.
#
# Columns (one row per restriction and FIR)
#   status        closed                    the State has closed its airspace to civil aviation
#                 avoided_by_eu_carriers    a ban or prohibition that applies to (EU / European) operators only;
#                                           other airlines may fly there legally (e.g. Russia for EU carriers)
#                 conflict_zone_advisory    an EASA Conflict Zone Information Bulletin (a recommendation, not a ban)
#   severity      closed | do_not_operate | below_flight_level | caution
#   scope         whole FIR | part of FIR (we only have FIR polygons, so "part of FIR" rows are approximations)
#   min_safe_fl   for below_flight_level: do not operate BELOW this flight level (FL320 = 32,000 ft); NULL = all levels
#   applies_to    who it applies to (text) and applies_to_countries: registration countries as OpenSky names them
#                 (origin_country), or ALL. OpenSky's origin_country is the country of REGISTRATION derived from the
#                 transponder address – not the airline's nationality (leased aircraft!). It is a proxy.
#   avoid_on_routes  TRUE when an operator it applies to should not cross the FIR at cruise level at all (closed, or
#                 "do not operate at all levels" in the whole FIR or nearly all of it). 08_gold_fir_traffic.py uses
#                 only these rows to explain detours; partial / below-FL / caution rows stay informational.
#   confidence    high   = checked against the primary source on LAST_REVIEWED
#                 medium = primary source for the measure, but its CURRENT status could not be confirmed, or a detail
#                          is taken from a secondary source
#                 low    = secondary sources only / unverified – shown for discussion, do not rely on it
#
# Sources (checked on LAST_REVIEWED):
#   EASA CZIB list: https://www.easa.europa.eu/en/domains/air-operations/czibs  (each CZIB has its own page)
#   Rosaviatsia restrictions for carriers of 36 States (28 Feb 2022) and Switzerland (1 Mar 2022), as reported by
#     airport-technology.com (2022-03-01) and Interfax (2022-03-01) – no 2026 primary source found.
#   FAA prohibitions for US operators: https://www.faa.gov/air_traffic/publications/us_restrictions (not reachable
#     for the review – FAA entries are therefore NOT in the list, see the note at the end of RESTRICTIONS).
#
# Setting: geo.restricted_interval (default "24 hours"). No external calls.
import pandas as pd
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")

LAST_REVIEWED = "2026-10-19"
CZIB_URL = "https://www.easa.europa.eu/en/domains/air-operations/czibs/"

# registration countries, spelled like OpenSky's origin_country (it uses "Kingdom of the Netherlands")
EU27 = ["Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Denmark", "Estonia", "Finland",
        "France", "Germany", "Greece", "Hungary", "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta",
        "Kingdom of the Netherlands", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain",
        "Sweden"]
EASA_STATES = EU27 + ["Iceland", "Liechtenstein", "Norway", "Switzerland"]
# Rosaviatsia, 28 Feb 2022: carriers of 36 States (EU27, UK, Canada, Norway, Iceland, Albania, Anguilla, British
# Virgin Islands, Gibraltar, Jersey), + Switzerland on 1 Mar 2022. The UK territories register aircraft with UK
# transponder blocks, so "United Kingdom" covers them in OpenSky.
RUSSIA_BAN = EU27 + ["United Kingdom", "Canada", "Norway", "Iceland", "Albania", "Switzerland"]

UKRAINE = ["UKLV", "UKBV", "UKDV", "UKFV", "UKOV"]
# Russian FIRs as they exist in the VATSIM boundaries (real Russia has more, smaller FIRs; VATSIM merges them).
# ICAO prefixes UE, UH, UI, UL, UN, UR, US, UU, UW are Russia – and UMKK (Kaliningrad), while the rest of UM is
# Belarus.
RUSSIA = ["UUWV", "ULLL", "URRV", "UWWW", "USSV", "USTV", "UNNT", "UNKL", "UIII", "UEEE", "UHHH", "UHMM", "UMKK"]
GULF = ["OBBB", "OKAC", "OTDF", "OMAE", "OOMM"]

RESTRICTIONS = [
    dict(restriction_id="UA-CLOSED", fir_icaos=UKRAINE, country="Ukraine", status="closed", severity="closed",
         scope="whole FIR", min_safe_fl=None, applies_to="all civil aviation", applies_to_countries=["ALL"],
         avoid_on_routes=True, valid_from="2022-02-24", valid_until=None, confidence="high",
         source="Ukrainian NOTAMs (airspace closed to civil aviation since 24 Feb 2022); EASA CZIB-2022-01R14 "
                "(revised 2026-07-24, valid until 2027-01-31): do not operate, all levels",
         source_url=CZIB_URL + "czib-2022-01r14",
         note="UIR Kyiv (UKBU) is not a separate VATSIM boundary; Simferopol (UKFV) included."),
    dict(restriction_id="BY-EU", fir_icaos=["UMMV"], country="Belarus", status="avoided_by_eu_carriers",
         severity="do_not_operate", scope="whole FIR", min_safe_fl=None,
         applies_to="operators with their principal place of business in an EASA Member State",
         applies_to_countries=EASA_STATES, avoid_on_routes=True, valid_from="2021-06-02", valid_until=None,
         confidence="high",
         source="EASA Safety Directives SD-2021-02 (2 Jun 2021, after the forced landing of Ryanair FR4978) and "
                "SD-2021-03, referenced in EASA CZIB-2022-01R14",
         source_url=CZIB_URL + "czib-2022-01r14",
         note="A prohibition for EASA operators, not a closure: Belarusian, Russian, Turkish, Chinese ... aircraft "
              "still fly there. UK/US measures not reviewed."),
    dict(restriction_id="RU-BAN", fir_icaos=RUSSIA, country="Russian Federation", status="avoided_by_eu_carriers",
         severity="closed", scope="whole FIR", min_safe_fl=None,
         applies_to="carriers of the EU27, UK (incl. Anguilla, BVI, Gibraltar, Jersey), Canada, Norway, Iceland, "
                    "Albania and Switzerland (Russian retaliation for the 2022 bans on Russian aircraft)",
         applies_to_countries=RUSSIA_BAN, avoid_on_routes=True, valid_from="2022-02-28", valid_until=None,
         confidence="medium",
         source="Rosaviatsia, 28 Feb 2022 (36 States) and 1 Mar 2022 (Switzerland), as reported by "
                "airport-technology.com / Interfax; the EU bans Russian aircraft in turn (Council Regulation (EU) "
                "2022/328)",
         source_url="https://www.airport-technology.com/news/russia-shuts-airspace/",
         note="Medium: no 2026 primary source found that confirms the ban is still in force (no lifting reported "
              "either). US carriers: not on the 2022 list; they avoid Russia as well (FAA prohibition NOTAM, Russian "
              "reciprocal measures reported) – NOT verified, so the United States is not in applies_to_countries."),
    dict(restriction_id="RU-CZIB", fir_icaos=["UUWV", "URRV", "ULLL", "USSV", "UWWW"], country="Russian Federation",
         status="conflict_zone_advisory", severity="do_not_operate", scope="part of FIR", min_safe_fl=None,
         applies_to="EASA operators and EASA-authorised third-country operators (TCO)",
         applies_to_countries=EASA_STATES, avoid_on_routes=False, valid_from="2025-01-09",
         valid_until="2027-01-31", confidence="high",
         source="EASA CZIB-2025-01 R3 (revised 2026-07-24): do not operate west of 60°E, all levels",
         source_url=CZIB_URL + "2025-01-r3",
         note="Only the part WEST of 60°E (FIR polygons are coarser). Comes on top of RU-BAN."),
    dict(restriction_id="IR-CZIB", fir_icaos=["OIIX"], country="Iran", status="conflict_zone_advisory",
         severity="do_not_operate", scope="whole FIR", min_safe_fl=None, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=True, valid_from="2026-07-08", valid_until="2026-11-16",
         confidence="high",
         source="EASA CZIB-2026-04-R2 (revised 2026-09-30): do not operate in FIR Tehran, all levels",
         source_url=CZIB_URL + "czib-2026-04-r2",
         note="Short-lived: valid until 2026-11-16 unless extended – check! US operators: FAA SFAR (not verified)."),
    dict(restriction_id="IQ-CZIB", fir_icaos=["ORBB"], country="Iraq", status="conflict_zone_advisory",
         severity="do_not_operate", scope="whole FIR", min_safe_fl=None, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=True, valid_from="2026-07-08", valid_until="2026-11-16",
         confidence="high",
         source="EASA CZIB-2026-05-R2 (revised 2026-09-30): do not operate in FIR Baghdad, all levels",
         source_url=CZIB_URL + "czib-2026-05-r2",
         note="Short-lived: valid until 2026-11-16 unless extended – check!"),
    dict(restriction_id="LB-CZIB", fir_icaos=["OLBB"], country="Lebanon", status="conflict_zone_advisory",
         severity="do_not_operate", scope="whole FIR", min_safe_fl=None, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=True, valid_from="2026-07-08", valid_until="2026-11-16",
         confidence="high",
         source="EASA CZIB-2026-06-R2 (revised 2026-09-30): do not operate, all levels, except to/from Beirut "
                "(OLBA) with arrival/departure over the sea",
         source_url=CZIB_URL + "czib-2026-06-r2", note="Flights to Beirut are allowed under conditions."),
    dict(restriction_id="GULF-CZIB", fir_icaos=GULF, country="Bahrain, Kuwait, Qatar, UAE, Oman",
         status="conflict_zone_advisory", severity="do_not_operate", scope="part of FIR", min_safe_fl=None,
         applies_to="EASA operators and TCO", applies_to_countries=EASA_STATES, avoid_on_routes=False,
         valid_from="2026-07-14", valid_until="2026-11-16", confidence="high",
         source="EASA CZIB-2026-07R3 (revised 2026-09-30): do not operate over the waters of the Persian Gulf and the "
                "Gulf of Oman west of 58°E, all levels; arrivals/departures at aerodromes in these FIRs exempt; "
                "caution over land",
         source_url=CZIB_URL + "czib-2026-07r3",
         note="Only the airspace over water – flights to Dubai, Doha, Bahrain ... continue, so not used for routes."),
    dict(restriction_id="SA-CZIB", fir_icaos=["OEJD"], country="Saudi Arabia", status="conflict_zone_advisory",
         severity="do_not_operate", scope="part of FIR", min_safe_fl=None, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=False, valid_from="2026-09-30", valid_until="2026-11-16",
         confidence="medium",
         source="EASA CZIB-2026-09-R1 (revised 2026-10-09): do not operate in the portions bounded by the FIR "
                "boundary and RIBOK-BOSUT-MIGDA-RAGNO-MIXUG-DEKLO-OBSAM-SILPA / ULBAM-TOTEB-DURMA-LAKSO-SOLAT; "
                "caution in the rest of FIR Jeddah",
         source_url=CZIB_URL + "czib-2026-09-r1",
         note="Medium: WHICH part of Saudi Arabia these waypoint lines enclose was not resolved in the review."),
    dict(restriction_id="JO-CZIB", fir_icaos=["OJAC"], country="Jordan", status="conflict_zone_advisory",
         severity="caution", scope="whole FIR", min_safe_fl=None, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=False, valid_from="2026-07-22", valid_until="2026-11-16",
         confidence="high",
         source="EASA CZIB-2026-08-R3 (revised 2026-10-14): risk assessment and contingency planning, no prohibition",
         source_url=CZIB_URL + "czib-2026-08-r3", note="Caution only – flying there is fine."),
    dict(restriction_id="SY-CZIB", fir_icaos=["OSTT"], country="Syria", status="conflict_zone_advisory",
         severity="do_not_operate", scope="part of FIR", min_safe_fl=260, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=False, valid_from="2017-03-31", valid_until="2026-11-30",
         confidence="high",
         source="EASA CZIB-2017-03R20 (revised 2026-10-01): do not operate west of ZELAF-BASEM-LOTAX-LATEB (all "
                "levels) and below FL260 in the rest of FIR Damascus",
         source_url=CZIB_URL + "czib-2017-03r20", note="Overflight at or above FL260 in the east is possible."),
    dict(restriction_id="YE-CZIB", fir_icaos=["OYSC"], country="Yemen", status="conflict_zone_advisory",
         severity="do_not_operate", scope="whole FIR", min_safe_fl=None, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=True, valid_from="2017-08-31", valid_until="2027-04-30",
         confidence="high",
         source="EASA CZIB-2017-07R20 (revised 2026-09-30): do not operate in FIR Sana'a, all levels, except the "
                "maritime part east of airway B400 (caution)",
         source_url=CZIB_URL + "czib-2017-07r20", note=None),
    dict(restriction_id="SD-CZIB", fir_icaos=["HSSS"], country="Sudan", status="conflict_zone_advisory",
         severity="do_not_operate", scope="whole FIR", min_safe_fl=None, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=True, valid_from="2023-04-26", valid_until="2027-01-31",
         confidence="high", source="EASA CZIB-2023-01R8 (revised 2026-07-24): do not operate over Sudan, all levels",
         source_url=CZIB_URL + "czib-2023-01r8", note=None),
    dict(restriction_id="LY-CZIB", fir_icaos=["HLLL"], country="Libya", status="conflict_zone_advisory",
         severity="below_flight_level", scope="whole FIR", min_safe_fl=320, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=False, valid_from="2017-03-31", valid_until="2027-01-31",
         confidence="high",
         source="EASA CZIB-2017-02R20 (revised 2026-07-24): do not operate below FL320 in FIR Tripoli",
         source_url=CZIB_URL + "czib-2017-02r20", note="Overflight at FL320 and above with caution."),
    dict(restriction_id="AF-CZIB", fir_icaos=["OAKX"], country="Afghanistan", status="conflict_zone_advisory",
         severity="below_flight_level", scope="whole FIR", min_safe_fl=320, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=False, valid_from="2017-10-17", valid_until="2027-01-31",
         confidence="high", source="EASA CZIB-2017-08R19 (revised 2026-07-24): do not operate below FL320 in FIR Kabul",
         source_url=CZIB_URL + "czib-2017-08r19", note=None),
    dict(restriction_id="PK-CZIB", fir_icaos=["OPKR", "OPLR"], country="Pakistan", status="conflict_zone_advisory",
         severity="below_flight_level", scope="part of FIR", min_safe_fl=260, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=False, valid_from="2018-01-10", valid_until="2027-01-31",
         confidence="high",
         source="EASA CZIB-2018-02R21 (revised 2026-07-24): do not operate below FL260 over Baluchistan and "
                "Khyber Pakhtunkhwa",
         source_url=CZIB_URL + "czib-2018-02r21", note="Only two provinces – the FIR polygons are much bigger."),
    dict(restriction_id="SO-CZIB", fir_icaos=["HCSM"], country="Somalia", status="conflict_zone_advisory",
         severity="below_flight_level", scope="whole FIR", min_safe_fl=260, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=False, valid_from="2017-03-31", valid_until="2027-04-30",
         confidence="high",
         source="EASA CZIB-2017-05R20 (revised 2026-09-30): do not operate at or below FL260 in FIR Mogadishu",
         source_url=CZIB_URL + "czib-2017-05r20", note="'At or below' FL260."),
    dict(restriction_id="ML-CZIB", fir_icaos=["DRRR", "GOOO"], country="Mali", status="conflict_zone_advisory",
         severity="below_flight_level", scope="part of FIR", min_safe_fl=260, applies_to="EASA operators and TCO",
         applies_to_countries=EASA_STATES, avoid_on_routes=False, valid_from="2017-03-31", valid_until="2027-04-30",
         confidence="high",
         source="EASA CZIB-2017-01R21 (revised 2026-09-30): do not operate below FL260 in the parts of FIR Niamey "
                "and FIR Dakar over Mali",
         source_url=CZIB_URL + "czib-2017-01r21", note="Only the part over Mali – DRRR and GOOO are much bigger."),
    # NOT in the list (on purpose – add them once you have verified them):
    #   * FAA prohibitions for US operators (SFARs / prohibition NOTAMs for e.g. Iran, Iraq, parts of Russia,
    #     North Korea): the FAA page could not be read during the review.
    #   * Israel (LLLL): the EASA CZIBs for Israel are WITHDRAWN as of the review; individual airlines decide.
    #   * EASA "Information Notes" for medium-risk airspace are not public.
]

SCHEMA = ("restriction_id STRING, fir_icao STRING, country STRING, status STRING, severity STRING, scope STRING, "
          "min_safe_fl INT, applies_to STRING, applies_to_countries STRING, avoid_on_routes BOOLEAN, "
          "valid_from DATE, valid_until DATE, expired BOOLEAN, confidence STRING, source STRING, source_url STRING, "
          "note STRING, last_reviewed DATE, days_since_review INT, fir_in_boundaries BOOLEAN")


def _cols(schema):
    return [c.split(" ")[0] for c in schema.split(", ")]


def build_restricted_airspace(restrictions=None, today=None, pdf_firs=None):
    """The curated list -> one row per restriction and FIR (pandas).

    today: date used for `expired` and `days_since_review` (default: now, UTC).
    pdf_firs: optional silver.firs – fir_in_boundaries tells whether the FIR code exists in the boundary data
    (a restriction on a FIR we have no polygon for can never match a position). NULL when not given.
    """
    restrictions = RESTRICTIONS if restrictions is None else restrictions
    today = pd.Timestamp(today or pd.Timestamp.now(tz="UTC").date()).normalize()
    known = set(pdf_firs["fir_icao"]) if pdf_firs is not None and not pdf_firs.empty else None
    rows = []
    for r in restrictions:
        for fir in r["fir_icaos"]:
            rows.append({k: v for k, v in r.items() if k != "fir_icaos"} | {"fir_icao": fir})
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=_cols(SCHEMA))
    df["applies_to_countries"] = df["applies_to_countries"].map(lambda c: ",".join(c))
    df["valid_from"] = pd.to_datetime(df["valid_from"]).dt.date
    until = pd.to_datetime(df["valid_until"])
    df["valid_until"] = until.dt.date
    df["expired"] = (until < today).fillna(False)
    df["last_reviewed"] = pd.Timestamp(LAST_REVIEWED).date()
    df["days_since_review"] = (today - pd.Timestamp(LAST_REVIEWED)).days
    df["min_safe_fl"] = df["min_safe_fl"].astype("Int64")
    df["fir_in_boundaries"] = df["fir_icao"].isin(known) if known is not None else None
    return df[_cols(SCHEMA)]

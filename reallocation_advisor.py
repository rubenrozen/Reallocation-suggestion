"""
Portfolio Reallocation Advisor
Runs on the last business day of each month.
3 phases : data collection → macro research (Sonnet+web) → analysis (Opus)
Sends one email per portfolio with full analysis and reallocation suggestions.
"""

import os
import json
import math
import smtplib
import anthropic
import gspread
from datetime import datetime, date
from calendar import monthrange
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2.service_account import Credentials


# ── Configuration ─────────────────────────────────────────────────────────────

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

SPREADSHEET_SECRETS = [
    "PORTFOLIO",
    "NEXT_HORIZON",
    "TREND_SPOTTING",
    "VALUE_UNDERFLOW"
]

ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
GMAIL_ADDRESS      = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
NOTIFY_EMAIL       = "ruben1rozenfeld@gmail.com"

MODEL_RESEARCH  = "claude-sonnet-4-6"   # Phase 2 — web search
MODEL_ANALYSIS  = "claude-opus-4-6"     # Phase 3 — deep analysis

CURRENCY_ROWS = {
    "€":   "J23",
    "$":   "J24",
    "¥":   "J25",
    "£":   "J26",
    "ILS": "J27",
    "CHF": "J28",
    "CNY": "J29",
    "SEK": "J30",
    "CAD": "J31",
}


# ── Date check ────────────────────────────────────────────────────────────────

def is_last_business_day():
    """Returns True if today is the last business day of the month."""
    today = date.today()
    last_day = monthrange(today.year, today.month)[1]

    # Find the last business day
    for d in range(last_day, 0, -1):
        candidate = date(today.year, today.month, d)
        if candidate.weekday() < 5:  # 0-4 = Mon-Fri
            return today == candidate

    return False


# ── Google Sheets ─────────────────────────────────────────────────────────────

def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not creds_json:
        raise RuntimeError("Secret GOOGLE_CREDENTIALS manquant.")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def safe_get(sheet, cell):
    """Read a single cell safely."""
    try:
        val = sheet.acell(cell).value
        return val if val is not None else ""
    except Exception:
        return ""


def safe_get_range(sheet, range_name):
    """Read a range and return flat list of non-empty values."""
    try:
        values = sheet.get(range_name)
        return [row[0] for row in values if row and str(row[0]).strip()]
    except Exception:
        return []


def read_portfolio_data(ss):
    """
    Phase 1 — Read all portfolio data from Google Sheets.
    Returns a structured dict with all relevant data.
    """
    try:
        portfolio   = ss.worksheet("Portfolio")
        lib         = ss.worksheet("Portfolio library")
    except Exception as e:
        raise RuntimeError(f"Impossible d'ouvrir les feuilles : {e}")

    # Strategy description
    strategy = safe_get(lib, "T4")

    # Equities
    equity_tickers = safe_get_range(portfolio, "O6:O")
    equity_qty     = safe_get_range(portfolio, "R6:R")
    equities = [
        {"ticker": t, "quantity": q}
        for t, q in zip(equity_tickers, equity_qty)
        if t
    ]

    # Bonds
    bond_tickers = safe_get_range(portfolio, "AM6:AM")
    bond_qty     = safe_get_range(portfolio, "AY6:AY")
    bonds = [
        {"ticker": t, "quantity": q}
        for t, q in zip(bond_tickers, bond_qty)
        if t
    ]

    # Crypto
    crypto_tickers = safe_get_range(portfolio, "V6:V")
    crypto_values  = safe_get_range(portfolio, "X6:X")
    cryptos = [
        {"ticker": t, "value_eur": v}
        for t, v in zip(crypto_tickers, crypto_values)
        if t
    ]

    # Futures
    futures_tickers = safe_get_range(portfolio, "Z6:Z")
    futures_values  = safe_get_range(portfolio, "AG6:AG")
    futures = [
        {"ticker": t, "value_eur": v}
        for t, v in zip(futures_tickers, futures_values)
        if t
    ]

    # Cash
    cash_available = safe_get(portfolio, "I22")

    # Currency exposures
    currency_exposure = {}
    currency_names    = safe_get_range(portfolio, "H23:H31")
    currency_values   = safe_get_range(portfolio, "J23:J31")
    for name, val in zip(currency_names, currency_values):
        if name:
            currency_exposure[name] = val

    # Asset count and max allowed
    total_assets = len(equities) + len(bonds) + len(cryptos) + len(futures)
    max_assets   = math.ceil(total_assets * 1.15)

    return {
        "strategy":           strategy,
        "equities":           equities,
        "bonds":              bonds,
        "cryptos":            cryptos,
        "futures":            futures,
        "cash_available":     cash_available,
        "currency_exposure":  currency_exposure,
        "total_assets":       total_assets,
        "max_assets":         max_assets,
        "date":               datetime.today().strftime("%B %d, %Y"),
    }


def format_portfolio_for_prompt(data):
    """Format portfolio data as a clean text block for AI prompts."""

    lines = [
        f"=== PORTFOLIO SNAPSHOT — {data['date']} ===",
        "",
        f"STRATEGY DESCRIPTION:",
        f"{data['strategy']}",
        "",
        f"CURRENT HOLDINGS ({data['total_assets']} assets | max allowed after reallocation: {data['max_assets']})",
        "",
        "EQUITIES:",
    ]
    if data["equities"]:
        for e in data["equities"]:
            lines.append(f"  {e['ticker']} — qty: {e['quantity']}")
    else:
        lines.append("  (none)")

    lines += ["", "BONDS:"]
    if data["bonds"]:
        for b in data["bonds"]:
            lines.append(f"  {b['ticker']} — qty: {b['quantity']}")
    else:
        lines.append("  (none)")

    lines += ["", "CRYPTO:"]
    if data["cryptos"]:
        for c in data["cryptos"]:
            lines.append(f"  {c['ticker']} — value: {c['value_eur']} €")
    else:
        lines.append("  (none)")

    lines += ["", "FUTURES:"]
    if data["futures"]:
        for f in data["futures"]:
            lines.append(f"  {f['ticker']} — value: {f['value_eur']} €")
    else:
        lines.append("  (none)")

    lines += [
        "",
        f"CASH AVAILABLE: {data['cash_available']}",
        "",
        "CURRENCY EXPOSURE (cash + assets combined):",
    ]
    for currency, value in data["currency_exposure"].items():
        lines.append(f"  {currency}: {value}")

    return "\n".join(lines)


# ── Phase 2 — Macro research (Sonnet + web search) ────────────────────────────

def run_macro_research(client, portfolio_data):
    """
    Phase 2 — Sonnet with web search.
    Researches current macro conditions relevant to this specific portfolio.
    """
    print("  [Phase 2] Macro research via Sonnet + web search...")

    portfolio_text = format_portfolio_for_prompt(portfolio_data)

    prompt = f"""You are a senior macro analyst preparing a briefing for a portfolio manager.

Here is the portfolio you must analyze:

{portfolio_text}

Please conduct thorough web research and produce a structured macro briefing covering:

1. MACRO ENVIRONMENT: Current global macro conditions (rates, inflation, growth, central bank posture)
2. MARKET TRENDS: Key trends in equity, bond, crypto and futures markets right now
3. SECTOR ANALYSIS: For each sector represented in the portfolio, what is the current outlook?
4. RISKS: Top 3-5 macro risks relevant to this portfolio in the coming 4-6 weeks
5. OPPORTUNITIES: Sectors or asset classes currently offering attractive entry points aligned with the strategy
6. CURRENCY: Any significant FX dynamics that could affect the portfolio's currency exposures

Be specific, data-driven, and actionable. Reference recent data where possible."""

    try:
        response = client.messages.create(
            model=MODEL_RESEARCH,
            max_tokens=4000,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": prompt}]
        )

        # Extract text from response
        macro_brief = ""
        for block in response.content:
            if block.type == "text":
                macro_brief += block.text

        print(f"  [Phase 2] Macro brief generated ({len(macro_brief)} chars)")
        return macro_brief

    except Exception as e:
        print(f"  [Phase 2] Error: {e}")
        return f"Macro research unavailable: {e}"


# ── Phase 3 — Portfolio analysis + reallocation (Opus) ────────────────────────

def run_portfolio_analysis(client, portfolio_data, macro_brief):
    """
    Phase 3 — Opus deep analysis.
    Full portfolio analysis + reallocation recommendations.
    """
    print("  [Phase 3] Deep analysis via Opus...")

    portfolio_text = format_portfolio_for_prompt(portfolio_data)

    prompt = f"""You are a senior portfolio manager conducting the end-of-month reallocation review.

## PORTFOLIO DATA
{portfolio_text}

## MACRO RESEARCH BRIEF
{macro_brief}

---

Please provide a comprehensive end-of-month portfolio review and reallocation plan structured as follows:

### 1. PORTFOLIO ASSESSMENT
- Overall portfolio health and alignment with the stated strategy
- Performance attribution by asset class
- Risk concentration analysis (sector, geography, currency, asset class)
- Currency exposure assessment — flag any imbalances and suggest rebalancing if warranted

### 2. POSITIONS TO SELL OR REDUCE
For each suggested sale/reduction:
- Ticker and quantity to sell
- Rationale (thesis broken, better opportunity, risk management, overweight)
- Estimated proceeds

### 3. POSITIONS TO BUY OR ADD
For each suggested purchase:
- Ticker, asset class, and quantity
- Rationale aligned with the strategy
- Estimated cost
- Note: Total portfolio positions must not exceed {portfolio_data['max_assets']} assets

### 4. CURRENCY REBALANCING (if applicable)
- Suggest any FX conversions needed given the reallocation
- Specify amounts and direction (e.g. "Convert $15,000 → €" or "Convert €8,000 → $")

### 5. CASH FLOW SUMMARY
- Cash before reallocation: {portfolio_data['cash_available']}
- Estimated proceeds from sales
- Estimated cost of purchases
- Projected cash remaining after reallocation

### 6. RISK ASSESSMENT
- Key risks introduced or reduced by this reallocation
- Portfolio resilience score (1-10) before and after

### 7. EXECUTIVE SUMMARY
- 3-5 bullet points summarizing the key moves and rationale

Be precise, quantitative, and directly actionable. All recommendations must be consistent with the stated strategy."""

    try:
        response = client.messages.create(
            model=MODEL_ANALYSIS,
            max_tokens=6000,
            messages=[{"role": "user", "content": prompt}]
        )

        analysis = ""
        for block in response.content:
            if hasattr(block, "text"):
                analysis += block.text

        print(f"  [Phase 3] Analysis generated ({len(analysis)} chars)")
        return analysis

    except Exception as e:
        print(f"  [Phase 3] Error: {e}")
        return f"Analysis unavailable: {e}"


# ── Email ──────────────────────────────────────────────────────────────────────

def send_reallocation_email(label, portfolio_data, macro_brief, analysis):
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        print("  ⚠️  Email credentials missing, skipping notification.")
        return

    today_str = datetime.today().strftime("%B %d, %Y")
    subject   = f"📊 Monthly Reallocation Advisory — {label} — {today_str}"

    body = f"""MONTHLY PORTFOLIO REALLOCATION ADVISORY
{label} | {today_str}
{'='*60}

STRATEGY
{portfolio_data['strategy']}

PORTFOLIO OVERVIEW
Total assets: {portfolio_data['total_assets']} | Max after reallocation: {portfolio_data['max_assets']}
Cash available: {portfolio_data['cash_available']}

{'='*60}
SECTION 1 — MACRO RESEARCH BRIEF
{'='*60}

{macro_brief}

{'='*60}
SECTION 2 — PORTFOLIO ANALYSIS & REALLOCATION PLAN
{'='*60}

{analysis}

{'='*60}
Generated automatically by Portfolio Reallocation Advisor
"""

    msg = MIMEMultipart()
    msg["From"]    = GMAIL_ADDRESS
    msg["To"]      = NOTIFY_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        password = "".join(c for c in GMAIL_APP_PASSWORD.strip() if ord(c) < 128)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_ADDRESS, password)
            server.sendmail(GMAIL_ADDRESS, NOTIFY_EMAIL, msg.as_string())
        print(f"  📧 Email sent for {label}")
    except Exception as e:
        print(f"  ⚠️  Email error: {e}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    today = datetime.today().strftime("%d/%m/%Y")
    print(f"=== Portfolio Reallocation Advisor — {today} ===\n")

    # Check if today is the last business day of the month
    if not is_last_business_day():
        print("Not the last business day of the month. No action taken.")
        return

    print("✅ Last business day of the month confirmed. Running analysis...\n")

    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY missing.")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    gc     = get_gspread_client()

    for label in SPREADSHEET_SECRETS:
        spreadsheet_id = os.environ.get(label, "").strip()
        if not spreadsheet_id:
            print(f"  ⚠️  Secret {label} not configured, skipping.")
            continue

        print(f"\n{'─'*50}")
        print(f"Processing: {label}")
        print(f"{'─'*50}")

        try:
            ss = gc.open_by_key(spreadsheet_id)
        except Exception as e:
            print(f"  ❌ Cannot open spreadsheet: {e}")
            continue

        # Phase 1 — Read portfolio data
        print("  [Phase 1] Reading portfolio data...")
        try:
            portfolio_data = read_portfolio_data(ss)
            print(f"  [Phase 1] {portfolio_data['total_assets']} assets loaded "
                  f"(max after reallocation: {portfolio_data['max_assets']})")
        except Exception as e:
            print(f"  ❌ Phase 1 error: {e}")
            continue

        if not portfolio_data["strategy"]:
            print("  ⚠️  No strategy description found in Portfolio library!T4, skipping.")
            continue

        # Phase 2 — Macro research
        macro_brief = run_macro_research(client, portfolio_data)

        # Phase 3 — Deep analysis + reallocation
        analysis = run_portfolio_analysis(client, portfolio_data, macro_brief)

        # Send email
        send_reallocation_email(label, portfolio_data, macro_brief, analysis)

    print(f"\n=== Done ===")


if __name__ == "__main__":
    main()

"""
branch_analysis.py
==================
Branch Supervision Data Analysis Tool
Risk & Compliance Department — UCS SACCO

Handles: Till transactions, Journals, Petty Cash
Works with any branch — column names are detected automatically.

Run: streamlit run branch_analysis.py
"""

import io, re
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

st.set_page_config(page_title="Branch Supervision Analysis", layout="wide")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stSidebar"]{background:#1A2942}
[data-testid="stSidebar"] *{color:#e8eef8!important}
[data-testid="metric-container"]{background:#f4f8fd;border:0.5px solid #d0d9e8;border-radius:8px;padding:12px 16px}
.block-container{padding-top:1.2rem}
.flag-box{padding:10px 14px;border-radius:8px;margin:6px 0;font-size:13px;line-height:1.6}
.flag-critical{background:#fcebeb;border-left:4px solid #a32d2d;color:#791f1f}
.flag-high{background:#faeeda;border-left:4px solid #b45309;color:#633806}
.flag-info{background:#e6f1fb;border-left:4px solid #185fa5;color:#0c447c}
</style>
""", unsafe_allow_html=True)

# ── SESSION STATE ─────────────────────────────────────────────────────────────
for k,v in {
    "tills":{}, "journals_raw":None, "journals_parsed":None,
    "petty":None, "branch":"", "ran":False
}.items():
    if k not in st.session_state: st.session_state[k]=v


# ═══════════════════════════════════════════════════════════════════════════════
# CLEANING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False)
def clean_till(file_bytes, filename):
    """
    Clean a till transaction file.
    The file has: DATE TIME DETAILS FOLIO DEBIT CREDIT BALANCE REFERENCE
    Works regardless of branch — column detection is automatic.
    """
    engine = "xlrd" if filename.lower().endswith(".xls") else "openpyxl"
    df = pd.read_excel(io.BytesIO(file_bytes), engine=engine)
    df.columns = [str(c).strip().upper() for c in df.columns]

    # Remove opening balance row
    if "DETAILS" in df.columns:
        df = df[~df["DETAILS"].astype(str).str.contains("Opening Balance", case=False, na=False)]

    # Parse date
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    # Parse time — stored as string or timedelta
    if "TIME" in df.columns:
        def _parse_time(t):
            try:
                ts = str(t).strip()
                if ":" in ts:
                    parts = ts.split(":")
                    return int(parts[0])  # return hour as int
            except: pass
            return None
        df["HOUR"] = df["TIME"].apply(_parse_time)

    # Numeric amounts
    for col in ["DEBIT","CREDIT"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",",""), errors="coerce").fillna(0)

    # Unified amount — CREDIT is cash going out (withdrawal), DEBIT is cash coming in (request)
    df["AMOUNT"] = df["CREDIT"].where(df["CREDIT"]>0, df["DEBIT"]) if "CREDIT" in df.columns else 0

    # Transaction type from DETAILS
    if "DETAILS" in df.columns:
        det = df["DETAILS"].astype(str).str.upper()
        def _classify(d):
            if "CASH REQUEST" in d or "CCASHIER" in d: return "Cash Request"
            if "CASH WITHDRAWAL" in d or "CASH WITHDRAW" in d: return "Cash Withdrawal"
            if "JOURNAL" in d: return "Journal"
            if "LOAN" in d: return "Loan"
            if "DEPOSIT" in d: return "Deposit"
            return "Other"
        df["TXN_TYPE"] = det.apply(_classify)

    # Extract member name from DETAILS
    def _extract_member(d):
        d = str(d)
        m = re.search(r"Withdrawn By:\s*([A-Z][A-Z\s]+?)(?:\s*\)|$)", d, re.IGNORECASE)
        if m: return m.group(1).strip()
        return ""
    if "DETAILS" in df.columns:
        df["MEMBER_NAME"] = df["DETAILS"].apply(_extract_member)

    # After-hours flag (before 07:30 or after 17:00)
    if "HOUR" in df.columns:
        df["AFTER_HOURS"] = (df["HOUR"] < 8) | (df["HOUR"] >= 17)

    # Weekday
    if "DATE" in df.columns:
        df["WEEKDAY"] = df["DATE"].dt.day_name()
        df["IS_WEEKEND"] = df["WEEKDAY"].isin(["Saturday","Sunday"])

    return df


@st.cache_data(show_spinner=False)
def parse_journals(file_bytes, filename):
    """
    Parse the complex journal file structure.
    Each batch has a header row, leg rows, and a Created By/Approved By footer.
    Returns a flat DataFrame with one row per journal leg.
    """
    engine = "xlrd" if filename.lower().endswith(".xls") else "openpyxl"
    raw = pd.read_excel(io.BytesIO(file_bytes), engine=engine, header=None)

    records = []
    i = 0
    created_by = ""; approved_by = ""

    while i < len(raw):
        row = raw.iloc[i].tolist()
        cell0 = str(row[0]).strip()

        if cell0 == "BatchNo":
            batch_no    = str(row[1]).strip()
            trdate      = row[3]
            description = str(row[5]).strip()
            jtype       = str(row[7]).strip() if pd.notna(row[7]) else ""
            created_by  = ""; approved_by = ""
            i += 2  # skip the column header row

            legs = []
            while i < len(raw):
                leg = raw.iloc[i].tolist()
                cell = str(leg[0]).strip()
                if cell == "BatchNo":
                    break
                if cell.startswith("Created By"):
                    created_by  = str(leg[1]).strip()
                    approved_by = str(leg[5]).strip() if pd.notna(leg[5]) else ""
                    i += 1
                    break
                try:
                    debit  = float(leg[5]) if pd.notna(leg[5]) else 0
                    credit = float(leg[6]) if pd.notna(leg[6]) else 0
                    legs.append({
                        "BATCH_NO": batch_no, "DATE": trdate,
                        "DESCRIPTION": description, "JOURNAL_TYPE": jtype,
                        "DR_ACCOUNT": str(leg[1]).strip(), "DR_NAME": str(leg[2]).strip(),
                        "CR_ACCOUNT": str(leg[3]).strip(), "CR_NAME": str(leg[4]).strip(),
                        "DEBIT": debit, "CREDIT": credit,
                    })
                except: pass
                i += 1

            for leg in legs:
                leg["CREATED_BY"]  = created_by
                leg["APPROVED_BY"] = approved_by
                records.append(leg)
        else:
            i += 1

    df = pd.DataFrame(records)
    if df.empty: return df

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["AMOUNT"] = df["DEBIT"].where(df["DEBIT"]>0, df["CREDIT"])

    # Normalise description: remove Journal( prefix and ) suffix
    df["DESC_CLEAN"] = (df["DESCRIPTION"]
        .str.replace(r"^Journal\s*\(", "", regex=True, flags=re.IGNORECASE)
        .str.rstrip(")")
        .str.strip()
    )

    # Category
    def _cat(d):
        d = d.upper()
        if "CASH FROM BANK" in d or "CASH TO BANK" in d: return "Bank/Treasury"
        if "MARKETING PROGRAM" in d or "REFRESHMENTS" in d: return "Marketing"
        if "TRANSPORT" in d: return "Transport/Petty"
        if "LOAN REPAYMENT" in d or "CONTRIBUTION" in d: return "Member/Loan"
        if "FUNERAL" in d or "WELFARE" in d or "CIC" in d: return "Welfare/Insurance"
        if "GROCERIES" in d or "SUGAR" in d or "WATER" in d or "BROOM" in d: return "Petty Purchase"
        if "FUNDS" in d: return "Funds Transfer"
        return "Other"
    df["CATEGORY"] = df["DESC_CLEAN"].apply(_cat)

    # Maker-checker flags
    df["SAME_MAKER_CHECKER"] = df["CREATED_BY"].str.strip() == df["APPROVED_BY"].str.strip()
    df["NO_CHECKER"] = df["APPROVED_BY"].str.strip() == ""
    df["WEEKDAY"] = df["DATE"].dt.day_name()
    df["IS_WEEKEND"] = df["WEEKDAY"].isin(["Saturday","Sunday"])

    return df


@st.cache_data(show_spinner=False)
def clean_petty(file_bytes, filename):
    engine = "xlrd" if filename.lower().endswith(".xls") else "openpyxl"
    df = pd.read_excel(io.BytesIO(file_bytes), engine=engine)
    df.columns = [str(c).strip().upper() for c in df.columns]
    df = df[~df.get("DETAILS","").astype(str).str.contains("Opening Balance", case=False, na=False)]
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    for col in ["DEBIT","CREDIT"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",",""), errors="coerce").fillna(0)
    df["AMOUNT"] = df["CREDIT"].where(df["CREDIT"]>0, df["DEBIT"]) if "CREDIT" in df.columns else 0
    if "DETAILS" in df.columns:
        df["DESC_CLEAN"] = (df["DETAILS"].astype(str)
            .str.replace(r"^Journal\s*\(", "", regex=True, flags=re.IGNORECASE)
            .str.split("batch").str[0].str.rstrip(")").str.strip())
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# EXPORT
# ═══════════════════════════════════════════════════════════════════════════════

def _s(v):
    if isinstance(v, float):
        return f"MWK {v:,.2f}" if v > 1000 else f"{v:.2f}"
    if isinstance(v, int): return f"{v:,}"
    return str(v)

def _fmt_ws(ws, header_hex="FF1A3A5C"):
    WHITE = "FFFFFFFF"; ALT = "FFEEF4FB"
    thin = Side(style="thin", color="FFD0D0D0")
    bdr = Border(left=thin, right=thin, top=thin, bottom=thin)
    for cell in ws[1]:
        cell.fill = PatternFill("solid", fgColor=header_hex)
        cell.font = Font(bold=True, color=WHITE, size=9, name="Arial")
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = bdr
    ws.row_dimensions[1].height = 28
    ws.freeze_panes = "A2"
    alt = PatternFill("solid", fgColor=ALT)
    for r, row in enumerate(ws.iter_rows(min_row=2), 2):
        for cell in row:
            cell.border = bdr
            cell.font = Font(size=9, name="Arial")
            if r % 2 == 0:
                c = cell.fill.fgColor.rgb if cell.fill and cell.fill.fgColor else "00"
                if c in ("FFFFFFFF","FF000000","00000000"): cell.fill = alt
    for col in ws.columns:
        ml = max((len(str(c.value or "")) for c in col), default=8)
        ws.column_dimensions[col[0].column_letter].width = min(max(ml+2,10),42)


def build_export(tills, journals, petty, branch):
    buf = io.BytesIO()
    sheets = {}

    # Summary
    till_all = pd.concat(tills.values(), ignore_index=True) if tills else pd.DataFrame()
    total_txns = len(till_all)
    total_withdrawal = till_all["CREDIT"].sum() if "CREDIT" in till_all.columns else 0
    total_requests = till_all["DEBIT"].sum() if "DEBIT" in till_all.columns else 0
    after_hrs = till_all["AFTER_HOURS"].sum() if "AFTER_HOURS" in till_all.columns else 0
    weekend_t = till_all["IS_WEEKEND"].sum() if "IS_WEEKEND" in till_all.columns else 0

    j_rows = [
        ("BRANCH SUPERVISION ANALYSIS", ""),
        ("Branch", str(branch)),
        ("Generated", str(pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"))),
        ("Period", f"{till_all['DATE'].min().date() if not till_all.empty and 'DATE' in till_all else 'N/A'} to {till_all['DATE'].max().date() if not till_all.empty and 'DATE' in till_all else 'N/A'}"),
        ("",""),
        ("TILL SUMMARY",""),
        ("Total till transactions", str(total_txns)),
        ("Total cash withdrawals (MWK)", _s(total_withdrawal)),
        ("Total cash requests (MWK)", _s(total_requests)),
        ("After-hours transactions", str(int(after_hrs))),
        ("Weekend transactions", str(int(weekend_t))),
        ("",""),
    ]
    if journals is not None and not journals.empty:
        same_mc = journals["SAME_MAKER_CHECKER"].sum() if "SAME_MAKER_CHECKER" in journals else 0
        no_chk = journals["NO_CHECKER"].sum() if "NO_CHECKER" in journals else 0
        wknd_j = journals["IS_WEEKEND"].sum() if "IS_WEEKEND" in journals else 0
        j_rows += [
            ("JOURNAL SUMMARY",""),
            ("Total journal legs", str(len(journals))),
            ("Unique batches", str(journals["BATCH_NO"].nunique())),
            ("Total debited (MWK)", _s(journals["DEBIT"].sum())),
            ("Total credited (MWK)", _s(journals["CREDIT"].sum())),
            ("Same maker and checker (FINDING)", str(int(same_mc))),
            ("No checker recorded (FINDING)", str(int(no_chk))),
            ("Weekend journals", str(int(wknd_j))),
            ("",""),
        ]
    if petty is not None and not petty.empty:
        j_rows += [
            ("PETTY CASH SUMMARY",""),
            ("Total petty transactions", str(len(petty))),
            ("Total spent (MWK)", _s(petty["CREDIT"].sum() if "CREDIT" in petty else 0)),
        ]
    sheets["SUMMARY"] = (pd.DataFrame(j_rows, columns=["Item","Value"]), "FF1A3A5C")

    # Tills
    if not till_all.empty:
        disp = [c for c in ["DATE","WEEKDAY","HOUR","TXN_TYPE","MEMBER_NAME","AMOUNT",
                             "DEBIT","CREDIT","AFTER_HOURS","IS_WEEKEND","DETAILS"] if c in till_all.columns]
        sheets["ALL_TILL_TRANSACTIONS"] = (till_all[disp].sort_values("DATE"), "FF1E7E3E")
        ah = till_all[till_all.get("AFTER_HOURS", pd.Series(False, index=till_all.index))==True] if "AFTER_HOURS" in till_all else pd.DataFrame()
        if not ah.empty:
            sheets["AFTER_HOURS_TRANSACTIONS"] = (ah[disp], "FFA32D2D")
        wk = till_all[till_all.get("IS_WEEKEND", pd.Series(False, index=till_all.index))==True] if "IS_WEEKEND" in till_all else pd.DataFrame()
        if not wk.empty:
            sheets["WEEKEND_TRANSACTIONS"] = (wk[disp], "FFB45309")

    # Journals
    if journals is not None and not journals.empty:
        jdisp = [c for c in ["BATCH_NO","DATE","WEEKDAY","DESC_CLEAN","CATEGORY",
                              "CREATED_BY","APPROVED_BY","SAME_MAKER_CHECKER","NO_CHECKER",
                              "DR_NAME","CR_NAME","DEBIT","CREDIT"] if c in journals.columns]
        sheets["ALL_JOURNALS"] = (journals[jdisp].sort_values("DATE"), "FF185FA5")

        # Findings
        findings = []
        same = journals[journals.get("SAME_MAKER_CHECKER", pd.Series(False))==True]
        if not same.empty:
            for _,r in same[jdisp].iterrows():
                row = r.to_dict(); row["FINDING"] = "SAME MAKER AND CHECKER"
                findings.append(row)
        no_c = journals[journals.get("NO_CHECKER", pd.Series(False))==True]
        if not no_c.empty:
            for _,r in no_c[jdisp].iterrows():
                row = r.to_dict(); row["FINDING"] = "NO CHECKER RECORDED"
                findings.append(row)
        wknd = journals[journals.get("IS_WEEKEND", pd.Series(False))==True]
        if not wknd.empty:
            for _,r in wknd[jdisp].iterrows():
                row = r.to_dict(); row["FINDING"] = "WEEKEND JOURNAL"
                findings.append(row)
        if findings:
            sheets["JOURNAL_FINDINGS"] = (pd.DataFrame(findings), "FFA32D2D")

        # Marketing summary
        mkt = journals[journals["CATEGORY"]=="Marketing"]
        if not mkt.empty:
            mkt_sum = (mkt[mkt["DEBIT"]>0].groupby("DR_NAME")
                .agg(COUNT=("BATCH_NO","count"), TOTAL_MWK=("DEBIT","sum"))
                .reset_index().sort_values("TOTAL_MWK",ascending=False))
            sheets["MARKETING_PROGRAM_SUMMARY"] = (mkt_sum, "FF0F6E56")

    # Petty cash
    if petty is not None and not petty.empty:
        pcols = [c for c in ["DATE","DESC_CLEAN","AMOUNT","DEBIT","CREDIT","BALANCE","DETAILS"] if c in petty.columns]
        sheets["PETTY_CASH"] = (petty[pcols].sort_values("DATE"), "FF534AB7")

    # Write
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sname, (df, color) in sheets.items():
            df_out = df.copy()
            # Convert all columns to safe types for Arrow
            for col in df_out.columns:
                if df_out[col].dtype == object:
                    df_out[col] = df_out[col].astype(str)
                elif pd.api.types.is_bool_dtype(df_out[col]):
                    df_out[col] = df_out[col].map({True:"Yes",False:"No"})
            df_out.to_excel(writer, sheet_name=sname[:31], index=False)

    buf.seek(0)
    wb = load_workbook(buf)
    for sname, (_, color) in sheets.items():
        n = sname[:31]
        if n in wb.sheetnames: _fmt_ws(wb[n], color)
    out = io.BytesIO()
    wb.save(out); out.seek(0)
    return out.read()


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### Branch Supervision")
    st.markdown("**Data Analysis Tool**")
    st.divider()

    page = st.radio("Navigate", [
        "Upload Files",
        "Till Analysis",
        "Journal Analysis",
        "Petty Cash",
        "Fraud Flags",
        "Export Report",
    ], label_visibility="collapsed")

    st.divider()
    st.markdown("**Session**")
    branch = st.session_state.get("branch","")
    st.markdown(f"Branch: `{branch or 'not set'}`")
    nt = len(st.session_state["tills"])
    st.markdown(f"Tills loaded: {nt}")
    j = st.session_state["journals_parsed"]
    st.markdown(f"Journals: {'loaded' if j is not None and not j.empty else 'not loaded'}")
    p = st.session_state["petty"]
    st.markdown(f"Petty cash: {'loaded' if p is not None and not p.empty else 'not loaded'}")
    st.divider()
    if st.button("Reset session", use_container_width=True):
        for k in list(st.session_state.keys()): del st.session_state[k]
        st.rerun()
    st.caption("Risk & Compliance Department")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGES
# ═══════════════════════════════════════════════════════════════════════════════

def flag(text, level="high"):
    cls = {"critical":"flag-critical","high":"flag-high","info":"flag-info"}.get(level,"flag-info")
    st.markdown(f'<div class="flag-box {cls}">{text}</div>', unsafe_allow_html=True)


def show_table(df, height=400):
    df_disp = df.copy()
    for col in df_disp.columns:
        if pd.api.types.is_bool_dtype(df_disp[col]):
            df_disp[col] = df_disp[col].map({True:"Yes",False:"No"})
    st.dataframe(df_disp, width="stretch", height=height, hide_index=True)


# ── PAGE 1: Upload ────────────────────────────────────────────────────────────
if page == "Upload Files":
    st.title("Upload Branch Files")
    st.caption("Upload all files for a branch. The tool detects column names automatically — it works with any branch format.")

    col_b, _ = st.columns([1,3])
    with col_b:
        b = st.text_input("Branch name", value=st.session_state.get("branch",""), placeholder="e.g. Kasungu")
        if b: st.session_state["branch"] = b

    st.divider()
    c1, c2, c3 = st.columns(3)

    with c1:
        st.subheader("Till files")
        st.caption("Upload all till files (till 1, till 2, etc.). You can upload multiple.")
        till_files = st.file_uploader("Till files", type=["xls","xlsx"],
                                       accept_multiple_files=True, label_visibility="collapsed")
        if till_files:
            for f in till_files:
                with st.spinner(f"Cleaning {f.name}..."):
                    df = clean_till(f.read(), f.name)
                    st.session_state["tills"][f.name] = df
            st.success(f"{len(st.session_state['tills'])} till file(s) loaded.")
            for name, df in st.session_state["tills"].items():
                st.caption(f"{name}: {len(df):,} rows")

    with c2:
        st.subheader("Journal file")
        st.caption("The journal export from CBS. One file covering the full period.")
        j_file = st.file_uploader("Journal file", type=["xls","xlsx"], label_visibility="collapsed")
        if j_file:
            with st.spinner("Parsing journals..."):
                raw_bytes = j_file.read()
                jdf = parse_journals(raw_bytes, j_file.name)
                st.session_state["journals_raw"]    = raw_bytes
                st.session_state["journals_parsed"] = jdf
            if not jdf.empty:
                st.success(f"Journals loaded: {jdf['BATCH_NO'].nunique():,} batches, {len(jdf):,} legs.")
            else:
                st.error("Could not parse journal file. Check the format.")

    with c3:
        st.subheader("Petty cash file")
        st.caption("The petty cash account transaction file.")
        p_file = st.file_uploader("Petty cash file", type=["xls","xlsx"], label_visibility="collapsed")
        if p_file:
            with st.spinner("Cleaning petty cash..."):
                pdf = clean_petty(p_file.read(), p_file.name)
                st.session_state["petty"] = pdf
            st.success(f"Petty cash loaded: {len(pdf):,} rows.")

    if st.session_state["tills"]:
        st.divider()
        st.info("Files loaded. Use the sidebar to navigate to analysis pages.")


# ── PAGE 2: Till Analysis ─────────────────────────────────────────────────────
elif page == "Till Analysis":
    st.title("Till Transaction Analysis")

    if not st.session_state["tills"]:
        st.warning("No till files loaded. Go to Upload Files.")
        st.stop()

    tills = st.session_state["tills"]
    till_all = pd.concat(tills.values(), ignore_index=True)

    # Summary metrics
    st.subheader(f"{st.session_state.get('branch','')} — All Tills Combined")
    m1,m2,m3,m4,m5 = st.columns(5)
    with m1: st.metric("Total transactions", f"{len(till_all):,}")
    with m2: st.metric("Total withdrawals (MWK)", f"{till_all['CREDIT'].sum():,.0f}" if "CREDIT" in till_all else "N/A")
    with m3: st.metric("Total cash requests (MWK)", f"{till_all['DEBIT'].sum():,.0f}" if "DEBIT" in till_all else "N/A")
    with m4:
        ah = int(till_all["AFTER_HOURS"].sum()) if "AFTER_HOURS" in till_all else 0
        st.metric("After-hours", f"{ah:,}", delta="Investigate" if ah>0 else None, delta_color="inverse")
    with m5:
        wk = int(till_all["IS_WEEKEND"].sum()) if "IS_WEEKEND" in till_all else 0
        st.metric("Weekend transactions", f"{wk:,}", delta="Investigate" if wk>0 else None, delta_color="inverse")

    st.divider()

    # Per-till breakdown
    tab_sum, tab_detail, tab_member = st.tabs(["Per-till summary", "Transaction detail", "Member analysis"])

    with tab_sum:
        rows = []
        for name, df in tills.items():
            rows.append({
                "Till": name,
                "Transactions": len(df),
                "Cash Requests (MWK)": df["DEBIT"].sum() if "DEBIT" in df else 0,
                "Withdrawals (MWK)": df["CREDIT"].sum() if "CREDIT" in df else 0,
                "After-hours": int(df["AFTER_HOURS"].sum()) if "AFTER_HOURS" in df else 0,
                "Weekend txns": int(df["IS_WEEKEND"].sum()) if "IS_WEEKEND" in df else 0,
            })
        summary_df = pd.DataFrame(rows)
        show_table(summary_df, height=300)

        if "DATE" in till_all.columns and "CREDIT" in till_all.columns:
            daily = (till_all[till_all["CREDIT"]>0]
                     .groupby(till_all["DATE"].dt.date)["CREDIT"]
                     .sum().reset_index())
            daily.columns = ["Date","Total Withdrawals (MWK)"]
            fig = px.bar(daily, x="Date", y="Total Withdrawals (MWK)",
                         title="Daily total cash withdrawals", height=320)
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with tab_detail:
        # Filters
        fc1, fc2 = st.columns(2)
        with fc1:
            selected_till = st.selectbox("Filter by till", ["All"] + list(tills.keys()))
        with fc2:
            txn_type = st.selectbox("Transaction type", ["All"] + sorted(till_all["TXN_TYPE"].unique().tolist()) if "TXN_TYPE" in till_all else ["All"])

        view = till_all.copy()
        if selected_till != "All":
            view = tills[selected_till].copy()
        if txn_type != "All" and "TXN_TYPE" in view:
            view = view[view["TXN_TYPE"] == txn_type]

        disp_cols = [c for c in ["DATE","WEEKDAY","HOUR","TXN_TYPE","MEMBER_NAME",
                                  "AMOUNT","AFTER_HOURS","IS_WEEKEND","DETAILS"] if c in view.columns]
        st.caption(f"Showing {len(view):,} rows")
        show_table(view[disp_cols] if disp_cols else view)

    with tab_member:
        if "MEMBER_NAME" in till_all.columns and "CREDIT" in till_all.columns:
            mdf = (till_all[till_all["CREDIT"]>0]
                   .groupby("MEMBER_NAME")
                   .agg(COUNT=("CREDIT","count"), TOTAL=("CREDIT","sum"), MAX=("CREDIT","max"))
                   .reset_index()
                   .sort_values("TOTAL", ascending=False)
                   .head(30))
            mdf.columns = ["Member","Withdrawal count","Total (MWK)","Largest single (MWK)"]
            st.caption("Top 30 members by total withdrawals")
            show_table(mdf, height=500)

            fig = px.bar(mdf.head(15), x="Member", y="Total (MWK)",
                         title="Top 15 members by total withdrawals", height=320)
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)


# ── PAGE 3: Journal Analysis ──────────────────────────────────────────────────
elif page == "Journal Analysis":
    st.title("Journal Analysis")

    jdf = st.session_state["journals_parsed"]
    if jdf is None or jdf.empty:
        st.warning("No journal file loaded. Go to Upload Files.")
        st.stop()

    # Summary
    same_mc = int(jdf["SAME_MAKER_CHECKER"].sum()) if "SAME_MAKER_CHECKER" in jdf else 0
    no_chk  = int(jdf["NO_CHECKER"].sum()) if "NO_CHECKER" in jdf else 0
    weekend = int(jdf["IS_WEEKEND"].sum()) if "IS_WEEKEND" in jdf else 0

    m1,m2,m3,m4,m5 = st.columns(5)
    with m1: st.metric("Total batches", f"{jdf['BATCH_NO'].nunique():,}")
    with m2: st.metric("Total legs", f"{len(jdf):,}")
    with m3: st.metric("Total debits (MWK)", f"{jdf['DEBIT'].sum():,.0f}")
    with m4: st.metric("Same maker/checker", f"{same_mc}", delta="Finding" if same_mc>0 else None, delta_color="inverse")
    with m5: st.metric("No checker", f"{no_chk}", delta="Finding" if no_chk>0 else None, delta_color="inverse")

    st.divider()
    tab1, tab2, tab3, tab4 = st.tabs(["All journals", "Maker-checker analysis", "By category", "By person"])

    with tab1:
        disp = [c for c in ["BATCH_NO","DATE","WEEKDAY","DESC_CLEAN","CATEGORY",
                             "CREATED_BY","APPROVED_BY","SAME_MAKER_CHECKER","NO_CHECKER",
                             "DR_NAME","CR_NAME","DEBIT","CREDIT"] if c in jdf.columns]
        show_table(jdf[disp], height=500)

    with tab2:
        st.markdown("**Maker-checker principle:** Every journal must be created by one person and approved by a DIFFERENT person. The same person creating and approving is a control failure.")

        if same_mc > 0:
            flag(f"{same_mc} journal leg(s) where the creator and approver are the same person.", "critical")
            same_rows = jdf[jdf["SAME_MAKER_CHECKER"]==True]
            disp = [c for c in ["BATCH_NO","DATE","DESC_CLEAN","CREATED_BY","APPROVED_BY","DEBIT","CREDIT"] if c in same_rows.columns]
            show_table(same_rows[disp].drop_duplicates("BATCH_NO"))
        else:
            st.success("No same maker-checker batches found.")

        if no_chk > 0:
            flag(f"{no_chk} journal leg(s) have no approver recorded.", "high")
            no_rows = jdf[jdf["NO_CHECKER"]==True]
            disp = [c for c in ["BATCH_NO","DATE","DESC_CLEAN","CREATED_BY","APPROVED_BY","DEBIT","CREDIT"] if c in no_rows.columns]
            show_table(no_rows[disp].drop_duplicates("BATCH_NO"))

        if weekend > 0:
            flag(f"{weekend} journal entries were made on weekends. These should be reviewed against physical authorisation.", "high")

    with tab3:
        if "CATEGORY" in jdf.columns:
            cat_sum = (jdf[jdf["DEBIT"]>0].groupby("CATEGORY")
                       .agg(COUNT=("BATCH_NO","nunique"), TOTAL_MWK=("DEBIT","sum"))
                       .reset_index().sort_values("TOTAL_MWK",ascending=False))
            c1, c2 = st.columns([1,2])
            with c1:
                show_table(cat_sum, height=300)
            with c2:
                fig = px.pie(cat_sum, values="TOTAL_MWK", names="CATEGORY",
                             title="Journal amounts by category", height=320, hole=0.4)
                st.plotly_chart(fig, use_container_width=True)

            # Marketing detail
            mkt = jdf[(jdf["CATEGORY"]=="Marketing") & (jdf["DEBIT"]>0)]
            if not mkt.empty:
                st.subheader("Marketing program payments by member")
                mkt_member = (mkt.groupby("DR_NAME")
                              .agg(COUNT=("BATCH_NO","count"), TOTAL=("DEBIT","sum"))
                              .reset_index().sort_values("TOTAL",ascending=False))
                show_table(mkt_member, height=250)

    with tab4:
        if "CREATED_BY" in jdf.columns:
            creator_sum = (jdf.groupby("CREATED_BY")
                           .agg(BATCHES=("BATCH_NO","nunique"), TOTAL_DEBIT=("DEBIT","sum"))
                           .reset_index().sort_values("TOTAL_DEBIT",ascending=False))
            approver_sum = (jdf[jdf["APPROVED_BY"].str.strip()!=""].groupby("APPROVED_BY")
                            .agg(APPROVED=("BATCH_NO","nunique"))
                            .reset_index().sort_values("APPROVED",ascending=False))
            c1, c2 = st.columns(2)
            with c1:
                st.caption("Journals created by person")
                show_table(creator_sum, height=300)
            with c2:
                st.caption("Journals approved by person")
                show_table(approver_sum, height=300)


# ── PAGE 4: Petty Cash ────────────────────────────────────────────────────────
elif page == "Petty Cash":
    st.title("Petty Cash Analysis")
    pdf = st.session_state["petty"]
    if pdf is None or pdf.empty:
        st.warning("No petty cash file loaded. Go to Upload Files.")
        st.stop()

    total_out = pdf["CREDIT"].sum() if "CREDIT" in pdf else 0
    total_in  = pdf["DEBIT"].sum() if "DEBIT" in pdf else 0

    m1,m2,m3 = st.columns(3)
    with m1: st.metric("Total transactions", f"{len(pdf):,}")
    with m2: st.metric("Total spent (MWK)", f"{total_out:,.2f}")
    with m3: st.metric("Total received (MWK)", f"{total_in:,.2f}")

    st.divider()
    disp = [c for c in ["DATE","DESC_CLEAN","AMOUNT","DEBIT","CREDIT","BALANCE","DETAILS"] if c in pdf.columns]
    show_table(pdf[disp] if disp else pdf)

    if "DESC_CLEAN" in pdf.columns and "CREDIT" in pdf.columns:
        desc_sum = (pdf[pdf["CREDIT"]>0].groupby("DESC_CLEAN")["CREDIT"]
                    .sum().reset_index().sort_values("CREDIT",ascending=False).head(15))
        desc_sum.columns = ["Description","Total (MWK)"]
        fig = px.bar(desc_sum, x="Description", y="Total (MWK)",
                     title="Petty cash expenditure by description", height=320)
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)


# ── PAGE 5: Fraud Flags ───────────────────────────────────────────────────────
elif page == "Fraud Flags":
    st.title("Fraud Detection Flags")
    st.caption("Automated flags for patterns that require investigation. Each finding must be verified against physical documents.")

    tills = st.session_state["tills"]
    jdf   = st.session_state["journals_parsed"]
    pdf   = st.session_state["petty"]
    branch = st.session_state.get("branch","")

    all_findings = []

    # ── TILL FLAGS ────────────────────────────────────────────────────────────
    if tills:
        till_all = pd.concat(tills.values(), ignore_index=True)

        st.subheader("Till findings")

        # After-hours
        if "AFTER_HOURS" in till_all.columns:
            ah = till_all[till_all["AFTER_HOURS"]==True]
            if not ah.empty:
                flag(f"{len(ah)} transactions processed outside business hours (before 08:00 or after 17:00). "
                     f"Total value: MWK {ah['AMOUNT'].sum():,.2f}. Verify these against till balances and authorisation.", "high")
                all_findings.append(("Till","After-hours transactions",len(ah)))

        # Weekend
        if "IS_WEEKEND" in till_all.columns:
            wk = till_all[till_all["IS_WEEKEND"]==True]
            if not wk.empty:
                flag(f"{len(wk)} till transactions on weekends. Verify the branch was open and authorised.", "high")
                all_findings.append(("Till","Weekend transactions",len(wk)))

        # Large single withdrawals
        if "CREDIT" in till_all.columns:
            large = till_all[till_all["CREDIT"] >= 1000000]
            if not large.empty:
                flag(f"{len(large)} single cash withdrawals of MWK 1,000,000 or more. "
                     f"Largest: MWK {large['CREDIT'].max():,.0f}. Verify each has signed authorisation.", "high")
                all_findings.append(("Till","Large withdrawals >=1M",len(large)))

    # ── JOURNAL FLAGS ─────────────────────────────────────────────────────────
    if jdf is not None and not jdf.empty:
        st.subheader("Journal findings")

        same_mc = jdf[jdf.get("SAME_MAKER_CHECKER", pd.Series(False))==True] if "SAME_MAKER_CHECKER" in jdf else pd.DataFrame()
        if not same_mc.empty:
            n = same_mc["BATCH_NO"].nunique()
            amt = same_mc["DEBIT"].sum()
            flag(f"MAKER-CHECKER VIOLATION: {n} journal batch(es) were created and approved by the same person. "
                 f"Total amount: MWK {amt:,.2f}. This is a direct control failure.", "critical")
            all_findings.append(("Journal","Same maker and checker",n))

        no_chk = jdf[jdf.get("NO_CHECKER", pd.Series(False))==True] if "NO_CHECKER" in jdf else pd.DataFrame()
        if not no_chk.empty:
            n = no_chk["BATCH_NO"].nunique()
            flag(f"NO CHECKER: {n} journal batch(es) have no approver recorded. "
                 f"These were entered without a second person reviewing.", "critical")
            all_findings.append(("Journal","No approver recorded",n))

        wk_j = jdf[jdf.get("IS_WEEKEND", pd.Series(False))==True] if "IS_WEEKEND" in jdf else pd.DataFrame()
        if not wk_j.empty:
            n = wk_j["BATCH_NO"].nunique()
            flag(f"WEEKEND JOURNALS: {n} journal batch(es) were posted on a Saturday or Sunday. "
                 f"Verify physical authorisation documents exist for these.", "high")
            all_findings.append(("Journal","Weekend journals",n))

        # Loan repayment journals (suspicious — loans should repay through normal channels)
        loan_j = jdf[jdf.get("CATEGORY","") == "Member/Loan"] if "CATEGORY" in jdf else pd.DataFrame()
        if not loan_j.empty:
            amt = loan_j["DEBIT"].sum()
            flag(f"LOAN REPAYMENT JOURNALS: {loan_j['BATCH_NO'].nunique()} loan repayment or contribution journals found. "
                 f"Total: MWK {amt:,.2f}. Verify these are legitimate and not used to manipulate loan balances.", "high")
            all_findings.append(("Journal","Loan/contribution journals",loan_j["BATCH_NO"].nunique()))

        # Linda Munthali as approver (normally creator — detected from real data)
        if "APPROVED_BY" in jdf.columns:
            creators = set(jdf["CREATED_BY"].str.strip().unique())
            for person in creators:
                if person == "": continue
                as_approver = jdf[jdf["APPROVED_BY"].str.strip()==person]
                if not as_approver.empty:
                    n = as_approver["BATCH_NO"].nunique()
                    flag(f"ROLE REVERSAL: {person} normally creates journals but also appears as approver for {n} batch(es). "
                         f"Verify whether this is authorised.", "high")
                    all_findings.append(("Journal",f"{person} as both creator and approver",n))

    # ── SUMMARY TABLE ─────────────────────────────────────────────────────────
    if all_findings:
        st.divider()
        st.subheader("Findings summary")
        findings_df = pd.DataFrame(all_findings, columns=["Source","Finding","Count"])
        show_table(findings_df, height=300)
    else:
        st.success("No automated fraud flags detected in the loaded data.")

    st.divider()
    flag("All flags above are automated detections based on data patterns. They must be verified against physical vouchers, requisition forms, and authorisation documents before being included in the supervision report.", "info")


# ── PAGE 6: Export ────────────────────────────────────────────────────────────
elif page == "Export Report":
    st.title("Export Analysis Report")

    tills  = st.session_state["tills"]
    jdf    = st.session_state["journals_parsed"]
    pdf    = st.session_state["petty"]
    branch = st.session_state.get("branch","BRANCH")

    if not tills and jdf is None and pdf is None:
        st.warning("No data loaded. Upload files first.")
        st.stop()

    st.markdown("""
    The exported workbook contains:
    - **SUMMARY** — key metrics and findings count
    - **ALL_TILL_TRANSACTIONS** — all till data cleaned and combined
    - **AFTER_HOURS_TRANSACTIONS** — transactions outside business hours
    - **WEEKEND_TRANSACTIONS** — Saturday and Sunday transactions
    - **ALL_JOURNALS** — all parsed journal entries
    - **JOURNAL_FINDINGS** — maker-checker violations and weekend journals flagged
    - **MARKETING_PROGRAM_SUMMARY** — marketing payments per member
    - **PETTY_CASH** — petty cash cleaned
    """)

    if st.button("Generate Excel Report", type="primary"):
        with st.spinner("Building workbook..."):
            excel = build_export(tills, jdf, pdf, branch)
            date_str = pd.Timestamp.now().strftime("%Y%m%d")
            fname = f"SUPERVISION_{branch.upper().replace(' ','_')}_{date_str}.xlsx"
            st.download_button(
                "Download Report",
                data=excel,
                file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.success(f"Report ready: {fname}")


"""
branch_analysis.py  —  Branch Supervision Data Analysis Tool
=============================================================
UCS SACCO  |  Risk & Compliance Department

Handles: Tills (any number), Treasury, Journals, Petty Cash
Works with any branch — all column detection is automatic.
Output matches Nkhatabay reference standard.

Run:   streamlit run branch_analysis.py
"""

import io, re, sys, os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

st.set_page_config(page_title="Branch Supervision Tool", layout="wide",
                   page_icon="", initial_sidebar_state="expanded")
st.markdown("""
<style>
[data-testid="stSidebar"]{background:#1A2942}
[data-testid="stSidebar"] *{color:#e8eef8!important}
[data-testid="metric-container"]{background:#f4f8fd;border:0.5px solid #d0d9e8;
  border-radius:8px;padding:12px 16px}
.block-container{padding-top:1rem}
.flag-critical{padding:10px 14px;border-radius:8px;margin:5px 0;font-size:13px;
  background:#fcebeb;border-left:4px solid #a32d2d;color:#791f1f}
.flag-high{padding:10px 14px;border-radius:8px;margin:5px 0;font-size:13px;
  background:#faeeda;border-left:4px solid #b45309;color:#633806}
.flag-info{padding:10px 14px;border-radius:8px;margin:5px 0;font-size:13px;
  background:#e6f1fb;border-left:4px solid #185fa5;color:#0c447c}
</style>""", unsafe_allow_html=True)

for k, v in {"branch":"","tills":{},"treasury":None,"journals":None,"petty":None}.items():
    if k not in st.session_state: st.session_state[k] = v

# ── helpers ────────────────────────────────────────────────────────────────────
def _num(s): return pd.to_numeric(s.astype(str).str.replace(",","").str.strip(),errors="coerce").fillna(0)
def _date(s): return pd.to_datetime(s,errors="coerce")
def _fmt(d):
    try: return pd.Timestamp(d).strftime("%d/%m/%Y")
    except: return str(d)
def flag(text,level="high"):
    cls={"critical":"flag-critical","high":"flag-high","info":"flag-info"}.get(level,"flag-info")
    st.markdown(f'<div class="{cls}">{text}</div>',unsafe_allow_html=True)
def show_df(df,height=420):
    d=df.copy()
    for c in d.columns:
        if pd.api.types.is_bool_dtype(d[c]): d[c]=d[c].map({True:"Yes",False:"No"})
    st.dataframe(d,width="stretch",height=height,hide_index=True)
def _member(det):
    for pat in [r"Withdrawn By:\s*([A-Z][A-Za-z\s]+?)(?:\s*[-\)]|$)",
                r"Deposited By:\s*([A-Z][A-Za-z\s]+?)(?:\s*[-\)]|$)",
                r"DIRECT RECEIPTS\(.*?-\s*Deposited By:\s*([A-Z][A-Za-z\s]+?)\)"]:
        m=re.search(pat,str(det),re.IGNORECASE)
        if m: return m.group(1).strip().title()
    return ""
def _cheque(det):
    # Cheque numbers appear in three narration styles across branches:
    #   'CHEQUE 2095' / 'CHEQUE NO 2095' / 'CHEQUE NUMBER 3837'  and the bare
    #   'CASH FROM BANK 3585' (no word 'cheque'). Capture all three.
    u=str(det).upper()
    m=re.search(r"(?:CHEQUE|CHQ)\s*(?:NO\.?|NUMBER)?\s*0*(\d{3,6})",u)
    if m: return m.group(1)
    m=re.search(r"FROM\s+BANK\s+0*(\d{3,6})\b",u)
    return m.group(1) if m else ""
def _teller_no(det):
    # 'Cash From TELLER 4 - ...' -> '4'. Blank for aggregate 'Cash To Tellers'.
    m=re.search(r"TELLER\s*0*(\d+)",str(det),re.IGNORECASE); return m.group(1) if m else ""
def _batch(det):
    m=re.search(r"batch\s*-\s*(\d+)",str(det),re.IGNORECASE)
    return m.group(1) if m else ""
def _voucher(det):
    m=re.search(r"Vno\.?\s*(\d{10,})",str(det),re.IGNORECASE)
    if not m: m=re.search(r"batch\s*-\s*(\d{10,})",str(det),re.IGNORECASE)
    return m.group(1) if m else ""
def _officer(ref):
    # REFERENCE looks like '031125081158ELIKU5334240198' -> the 4-6 letters
    # in the middle are the posting officer/teller code. This is our "who" signal.
    m=re.search(r"[A-Z]{4,6}",str(ref)); return m.group() if m else ""
def _balnum(b):
    # '3,000,000.00 DR' -> signed float (DR positive for a cash/asset account,
    # CR negative). Used for the running-balance integrity check.
    s=str(b).strip(); m=re.match(r"([\d,]+\.?\d*)\s*(DR|CR)?",s,re.I)
    if not m: return np.nan
    val=float(m.group(1).replace(",",""))
    return -val if (m.group(2) or "").upper()=="CR" else val
def _safe_cell(v):
    # SECURITY: stop Excel/CSV formula injection. Bank text beginning with
    # = + - @ would execute as a formula when opened. Force it to plain text.
    if isinstance(v,str) and v[:1] in ("=","+","-","@","\t","\r"): return "'"+v
    return v
def _ctrl_from_footer(fb,fn):
    # Crystal prints a footer: 'Closing Balance ... Total Debits : N ... Total
    # Credits : M ... End of Report'. We read it RAW (no header) and pull the two
    # numbers so we can later prove our parsed totals tie out to the statement.
    try:
        eng="xlrd" if fn.lower().endswith(".xls") else "openpyxl"
        raw=pd.read_excel(io.BytesIO(fb),engine=eng,header=None,dtype=str)
        for _,r in raw.iterrows():
            line=" ".join(str(x) for x in r.tolist() if pd.notna(x))
            if "total debits" in line.lower():
                d=re.search(r"total debits\s*:?\s*([\d,]+\.?\d*)",line,re.I)
                c=re.search(r"total credits\s*:?\s*([\d,]+\.?\d*)",line,re.I)
                return (float(d.group(1).replace(",","")) if d else np.nan,
                        float(c.group(1).replace(",","")) if c else np.nan)
    except Exception:
        pass
    return (np.nan,np.nan)

# ── cleaners ───────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def clean_till(fb,fn,label):
    eng="xlrd" if fn.lower().endswith(".xls") else "openpyxl"
    df=pd.read_excel(io.BytesIO(fb),engine=eng)
    df.columns=[str(c).strip().upper() for c in df.columns]
    if "DETAILS" in df.columns:
        bad=df["DETAILS"].astype(str).str.upper()
        df=df[~bad.str.contains("OPENING BALANCE",na=False)
              &~bad.str.contains("CLOSING BALANCE",na=False)
              &~bad.str.contains("END OF REPORT",na=False)
              &~bad.str.contains("TOTAL DEBITS",na=False)].copy()
    df["DATE"]=_date(df.get("DATE",pd.Series(dtype=str)))
    df=df[df["DATE"].notna()].copy()           # drop any non-transaction junk rows
    df["TIME_RAW"]=df.get("TIME",pd.Series(dtype=str)).astype(str)
    df["HOUR"]=df["TIME_RAW"].str.extract(r"^(\d{2}):",expand=False).astype(float)
    df["DEBIT"]=_num(df.get("DEBIT",pd.Series(0,index=df.index)))
    df["CREDIT"]=_num(df.get("CREDIT",pd.Series(0,index=df.index)))
    df["TILL"]=label; df["LINE_NO"]=range(1,len(df)+1)
    det=df.get("DETAILS",pd.Series("",index=df.index)).astype(str)
    def _cat(d):
        du=d.upper()
        if "REVERSAL" in du or du.startswith("REV-"): return "REVERSAL"
        if "CASH REQUEST" in du or "CCASHIER" in du: return "CASH_REQUEST"
        if "CASH WITHDRAWAL" in du or "CASH WITHDRAW" in du: return "WITHDRAWAL"
        if "CASH DEPOSIT" in du or "(CASH DEPOSIT)" in du: return "DEPOSIT"
        if any(x in du for x in ["DIRECT RECEIPT","LOAN FULL SETTLEMENT","LOAN PARTIAL",
                                   "FULL SETTLEMENT","PARTIAL PAYMENT"]): return "DIRECT_RECEIPT"
        if "TELLER" in du: return "TELLER_MOVEMENT"
        return "OTHER"
    df["CATEGORY"]=det.apply(_cat)
    df["MEMBER_NAME"]=det.apply(_member)
    df["VOUCHER_NO"]=det.apply(_voucher)
    df["DATE_FMT"]=df["DATE"].apply(_fmt)
    df["WEEKDAY"]=df["DATE"].dt.day_name()
    df["IS_WEEKEND"]=df["WEEKDAY"].isin(["Saturday","Sunday"])
    # after-hours tightened: only genuinely odd times (before 07:00 or from 19:00)
    # so normal opening/closing entries don't drown the real anomalies.
    df["AFTER_HOURS"]=df["HOUR"].notna()&((df["HOUR"]<7)|(df["HOUR"]>=19))
    df["TIME_DISPLAY"]=df["TIME_RAW"].str[:8]
    df["OFFICER"]=df.get("REFERENCE",pd.Series("",index=df.index)).apply(_officer)
    df["BALANCE_DISPLAY"]=df.get("BALANCE",pd.Series("",index=df.index)).astype(str)
    df["BALANCE_NUM"]=df["BALANCE_DISPLAY"].apply(_balnum)
    df.attrs["ctrl_deb"],df.attrs["ctrl_cre"]=_ctrl_from_footer(fb,fn)
    return df

@st.cache_data(show_spinner=False)
def clean_treasury(fb,fn):
    eng="xlrd" if fn.lower().endswith(".xls") else "openpyxl"
    df=pd.read_excel(io.BytesIO(fb),engine=eng)
    df.columns=[str(c).strip().upper() for c in df.columns]
    df=df[~df.get("DETAILS",pd.Series("",index=df.index)).astype(str).str.upper()
           .str.contains("OPENING BALANCE|CLOSING BALANCE|END OF REPORT|TOTAL DEBITS",
                         na=False,regex=True)].copy()
    df["DATE"]=_date(df.get("DATE",pd.Series(dtype=str)))
    df=df[df["DATE"].notna()].copy()
    df["DATE_FMT"]=df["DATE"].apply(_fmt)
    df["TIME_DISPLAY"]=df.get("TIME",pd.Series("",index=df.index)).astype(str).str[:8]
    df["DEBIT"]=_num(df.get("DEBIT",pd.Series(0,index=df.index)))
    df["CREDIT"]=_num(df.get("CREDIT",pd.Series(0,index=df.index)))
    df["LINE_NO"]=range(1,len(df)+1)
    det=df.get("DETAILS",pd.Series("",index=df.index)).astype(str)
    def _cat(d):
        du=d.upper()
        if "REVERSAL" in du or du.strip().upper().startswith("REV-"): return "REVERSAL"
        if "CASH FROM BANK" in du or ("WITHDRAW FROM" in du and "NBM" in du) \
                or ("CHEQUE" in du and "TREASURY" in du): return "CASH_FROM_BANK"
        if "CASH TO BANK" in du or "CASH DEPOSIT FROM" in du \
                or ("DEPOSIT" in du and "NBM" in du): return "CASH_TO_BANK"
        if "CASH TO TELLER" in du: return "CASH_TO_TELLERS"
        if "CASH FROM TELLER" in du: return "CASH_FROM_TELLERS"
        if "PETTY" in du: return "PETTY_CASH_TOPUP"
        if "DIRECT RECEIPT" in du: return "DIRECT_RECEIPTS"
        if "OVERAGE" in du or "SHORTAGE" in du: return "CASH_DIFFERENCE"
        return "OTHER"
    df["CATEGORY"]=det.apply(_cat)
    df["CHEQUE_NO"]=det.apply(_cheque)
    df["BATCH_NO"]=det.apply(_batch)
    df["DEPOSITOR"]=det.apply(_member)
    def _act(d):
        d=str(d)
        d=re.sub(r"\s*batch\s*-\s*\d+","",d,flags=re.IGNORECASE)
        d=re.sub(r"^Journal\s*\(","",d,flags=re.IGNORECASE).rstrip(")").strip()
        return d.strip()
    df["ACTIVITY"]=det.apply(_act)
    df["TELLER_NO"]=det.apply(_teller_no)
    df["OFFICER"]=df.get("REFERENCE",pd.Series("",index=df.index)).apply(_officer)
    df["BALANCE"]=df.get("BALANCE",pd.Series("",index=df.index)).astype(str)
    df["BALANCE_NUM"]=df["BALANCE"].apply(_balnum)
    df["REFERENCE"]=df.get("REFERENCE",pd.Series("",index=df.index)).astype(str)
    df.attrs["ctrl_deb"],df.attrs["ctrl_cre"]=_ctrl_from_footer(fb,fn)
    return df

@st.cache_data(show_spinner=False)
def parse_journals(fb,fn):
    eng="xlrd" if fn.lower().endswith(".xls") else "openpyxl"
    raw=pd.read_excel(io.BytesIO(fb),engine=eng,header=None)
    records=[]; i=0; created_by=""; approved_by=""
    while i<len(raw):
        row=raw.iloc[i].tolist()
        if str(row[0]).strip()=="BatchNo":
            batch_no=str(row[1]).strip(); trdate=row[3]
            desc=str(row[5]).strip(); jtype=str(row[7]).strip() if pd.notna(row[7]) else ""
            created_by=""; approved_by=""; i+=2; legs=[]
            while i<len(raw):
                leg=raw.iloc[i].tolist(); c0=str(leg[0]).strip()
                if c0=="BatchNo": break
                if c0.startswith("Created By"):
                    created_by=str(leg[1]).strip()
                    approved_by=str(leg[5]).strip() if pd.notna(leg[5]) else ""
                    i+=1; break
                try:
                    debit=float(leg[5]) if pd.notna(leg[5]) else 0
                    credit=float(leg[6]) if pd.notna(leg[6]) else 0
                    legs.append({"BATCH_NO":batch_no,"DATE":trdate,"DESCRIPTION":desc,
                                 "JOURNAL_TYPE":jtype,
                                 "DR_ACCOUNT":str(leg[1]).strip(),"DR_NAME":str(leg[2]).strip(),
                                 "CR_ACCOUNT":str(leg[3]).strip(),"CR_NAME":str(leg[4]).strip(),
                                 "DEBIT":debit,"CREDIT":credit})
                except: pass
                i+=1
            for leg in legs:
                leg["CREATED_BY"]=created_by; leg["APPROVED_BY"]=approved_by
                records.append(leg)
        else: i+=1
    if not records: return pd.DataFrame()
    df=pd.DataFrame(records)
    df["DATE"]=_date(df["DATE"]); df["DATE_FMT"]=df["DATE"].apply(_fmt)
    df["WEEKDAY"]=df["DATE"].dt.day_name()
    df["IS_WEEKEND"]=df["WEEKDAY"].isin(["Saturday","Sunday"])
    df["AMOUNT"]=df["DEBIT"].where(df["DEBIT"]>0,df["CREDIT"])
    df["DESC_CLEAN"]=(df["DESCRIPTION"]
        .str.replace(r"^Journal\s*\(","",regex=True,flags=re.IGNORECASE)
        .str.rstrip(")").str.strip())
    def _cat(d):
        du=d.upper()
        if "CASH FROM BANK" in du or ("NBM" in du and "TREASURY" in du): return "CASH_FROM_BANK"
        if "CASH TO BANK" in du or "CASH DEPOSIT" in du: return "CASH_TO_BANK"
        if "TRANSPORT" in du: return "TRANSPORT"
        if "PETTY" in du: return "PETTY_CASH"
        if "MARKETING" in du or "BAM" in du or "BRING A MEMBER" in du: return "MARKETING_BAM"
        if "MEMBER EDUCATION" in du: return "MEMBER_EDUCATION"
        if "CIC" in du or "FUNERAL" in du or "INSURANCE" in du: return "INSURANCE"
        if "ELECTRICITY" in du or "WATER" in du or "BILL" in du: return "UTILITIES"
        if "SALARY" in du or "PAYROLL" in du: return "SALARY"
        if any(x in du for x in ["GROCERIES","CREMORA","SUGAR","MARGARINE","TOILET",
                                   "MOPPER","PRESTIGE"]): return "OFFICE_SUPPLIES"
        if "GENSET" in du or "FUEL" in du or "GENERATOR" in du: return "FUEL_GENERATOR"
        if "SOS" in du or "SAVINGS" in du: return "SAVINGS_TRANSFER"
        return "OTHER"
    df["CATEGORY"]=df["DESC_CLEAN"].apply(_cat)
    df["SAME_MAKER_CHECKER"]=df["CREATED_BY"].str.strip()==df["APPROVED_BY"].str.strip()
    df["NO_CHECKER"]=df["APPROVED_BY"].str.strip()==""
    SYS=["TREASURY","NBM","PETTY CASH","CCASHIER","TELLER","BRING A MEMBER",
         "MARKETING","MEMBER EDUCATION","CIC FUNERAL","CASH","BANK"]
    def _sys(n): return any(k in str(n).upper() for k in SYS)
    df["PERSON_TO_PERSON"]=(~df["DR_NAME"].apply(_sys)&~df["CR_NAME"].apply(_sys)&(df["DEBIT"]>0))
    df["SAME_PERSON_DR_CR"]=(df["DR_NAME"].str.strip().str.upper()==df["CR_NAME"].str.strip().str.upper())&(df["DEBIT"]>0)
    return df

@st.cache_data(show_spinner=False)
def clean_petty(fb,fn):
    eng="xlrd" if fn.lower().endswith(".xls") else "openpyxl"
    df=pd.read_excel(io.BytesIO(fb),engine=eng)
    df.columns=[str(c).strip().upper() for c in df.columns]
    df=df[~df.get("DETAILS",pd.Series("",index=df.index)).astype(str).str.upper()
           .str.contains("OPENING BALANCE|CLOSING BALANCE|END OF REPORT|TOTAL DEBITS",
                         na=False,regex=True)].copy()
    df["DATE"]=_date(df.get("DATE",pd.Series(dtype=str)))
    df=df[df["DATE"].notna()].copy()
    df["DATE_FMT"]=df["DATE"].apply(_fmt)
    df["TIME_DISPLAY"]=df.get("TIME",pd.Series("",index=df.index)).astype(str).str[:8]
    df["DEBIT"]=_num(df.get("DEBIT",pd.Series(0,index=df.index)))
    df["CREDIT"]=_num(df.get("CREDIT",pd.Series(0,index=df.index)))
    df["AMOUNT"]=df["DEBIT"].where(df["DEBIT"]>0,df["CREDIT"])
    df["LINE_NO"]=range(1,len(df)+1)
    det=df.get("DETAILS",pd.Series("",index=df.index)).astype(str)
    def _cat(d):
        du=d.upper()
        if "REVERSAL" in du or du.strip().upper().startswith("REV-"): return "REVERSAL"
        if "TOPUP" in du or "TOP UP" in du or "TOP-UP" in du: return "TOPUP"
        if "TRANSPORT" in du: return "TRANSPORT"
        if any(x in du for x in ["GROCERIES","CREMORA","SUGAR","MARGARINE","TOILET",
                                   "MOPPER","PRESTIGE"]): return "GROCERIES_SUPPLIES"
        if "ELECTRICITY" in du or "WATER" in du or "BILL" in du: return "UTILITIES"
        if "GENSET" in du or "FUEL" in du or "GENERATOR" in du: return "FUEL_GENERATOR"
        if "PRINTER" in du or "TONER" in du or "STATIONERY" in du: return "STATIONERY"
        return "OTHER"
    df["CATEGORY"]=det.apply(_cat)
    def _act(d):
        d=str(d)
        d=re.sub(r"\s*batch\s*-\s*\d+","",d,flags=re.IGNORECASE)
        d=re.sub(r"^Journal\s*\(","",d,flags=re.IGNORECASE).rstrip(")").strip()
        return d.strip()
    df["ACTIVITY"]=det.apply(_act)
    df["BATCH_NO"]=det.apply(_batch)
    df["OFFICER"]=df.get("REFERENCE",pd.Series("",index=df.index)).apply(_officer)
    df["BALANCE"]=df.get("BALANCE",pd.Series("",index=df.index)).astype(str)
    df["BALANCE_NUM"]=df["BALANCE"].apply(_balnum)
    # ---- real-logic flagging (replaces the old flat 'every payment >= 20k') ----
    # A flat threshold flagged almost everything and buried the real exceptions.
    # Instead flag what is genuinely worth a second look:
    df["FLAG"]=""
    spend=df[df["CREDIT"]>0]["CREDIT"]
    # 1) statistical outlier: spend far above this branch's own norm
    #    (> 75th percentile + 1.5*IQR), so the threshold adapts per branch.
    if len(spend)>=4:
        q1,q3=spend.quantile(0.25),spend.quantile(0.75); hi=q3+3.0*(q3-q1)
        df.loc[(df["CREDIT"]>0)&(df["CREDIT"]>hi),"FLAG"]=(
            "Unusually large vs branch norm - verify voucher & approval")
    # 2) reversals: always worth confirming the original + reversal pair
    df.loc[df["CATEGORY"]=="REVERSAL","FLAG"]="Reversal - confirm original entry and reason"
    # 3) duplicate: same activity + amount + date (possible double payment)
    dup=df.duplicated(subset=["DATE","CREDIT","ACTIVITY"],keep=False)&(df["CREDIT"]>0)
    df.loc[dup&(df["FLAG"]==""),"FLAG"]="Possible duplicate - same activity, amount & date"
    # 4) spend with no batch reference (weak audit trail)
    df.loc[(df["CREDIT"]>0)&(df["BATCH_NO"]=="")&(df["FLAG"]==""),"FLAG"]=(
        "Spend with no batch reference - check supporting document")
    df.attrs["ctrl_deb"],df.attrs["ctrl_cre"]=_ctrl_from_footer(fb,fn)
    return df

# ── excel output engine (rewritten for clarity + real-logic flagging) ───────────
# Design goals:
#   * findings first  - sheet 00 lists every real exception, ranked by severity,
#     with where to look and what to do, so the analyst focuses on anomalies.
#   * numbers are numbers - all money is numeric with accounting format, so every
#     column sums, sorts and pivots. Text balances are kept only for reference.
#   * proof of completeness - each module ties its parsed totals to the statement
#     footer (Total Debits / Total Credits) and runs a running-balance check.
#   * security - every string cell is guarded against Excel formula injection.
from collections import Counter

NAVY="FF1A3A5C"; GREEN="FF1E7E3E"; AMBER="FFB45309"; BLUE="FF185FA5"
RED="FFB91C1C"; TEAL="FF0F6E56"; PURPLE="FF534AB7"; WHITE="FFFFFFFF"; ALT="FFEFF4FB"
SEV_FILL={"CRITICAL":"FFF6D4D4","HIGH":"FFFBE2C7","MEDIUM":"FFFBF1C7",
          "LOW":"FFE4EDF8","INFO":"FFE4EDF8"}
SEV_RANK={"CRITICAL":0,"HIGH":1,"MEDIUM":2,"LOW":3,"INFO":4}
MONEYFMT='#,##0.00;(#,##0.00);-'; INTFMT='#,##0'
# columns rendered as money / integer, detected by header name
MONEY_HINT=("AMOUNT","DEBIT","CREDIT","BALANCE","TOTAL","SPENT","DRAWN","MWK",
            "DEPOSITED","RECEIVED","NET","LARGEST","ISSUED","SHORTAGE","OVERAGE")
INT_COLS={"COUNT","ROWS","ROW_COUNT","TRANSACTIONS","WITHDRAWALS","DEPOSITS",
          "BATCHES","CHEQUE_NO","LINE_NO","TELLER_NO","APPROVED","DIRECT_RECEIPTS",
          "BAL_BREAKS","DUPLICATES","DEPOSIT_COUNT","WITHDRAWAL_COUNT"}

def _safe_df(df):
    d=df.copy()
    for col in d.columns:
        if pd.api.types.is_bool_dtype(d[col]): d[col]=d[col].map({True:"Yes",False:"No"})
        elif d[col].dtype==object:
            d[col]=d[col].astype(str).replace({"nan":"","NaT":"","None":""}).map(_safe_cell)
    return d

def _money_str(v):
    if isinstance(v,(int,float)) and not isinstance(v,bool):
        return f"MWK {v:,.2f}" if v else "MWK 0.00"
    return str(v)

class Findings:
    """Collects exceptions raised by the automated checks across all modules."""
    def __init__(self): self.items=[]
    def add(self,sev,area,finding,count="",amount="",where="",action=""):
        self.items.append({"SEVERITY":sev,"AREA":area,"FINDING":finding,"COUNT":count,
                           "AMOUNT_MWK":amount,"SEE_SHEET":where,"RECOMMENDED_ACTION":action})
    def df(self):
        if not self.items:
            return pd.DataFrame([{"SEVERITY":"INFO","AREA":"-",
                "FINDING":"No exceptions detected by automated checks","COUNT":"",
                "AMOUNT_MWK":"","SEE_SHEET":"","RECOMMENDED_ACTION":
                "Proceed with physical reconciliation"}])
        d=pd.DataFrame(self.items)
        d["_r"]=d["SEVERITY"].map(SEV_RANK).fillna(9)
        return d.sort_values("_r").drop(columns="_r").reset_index(drop=True)

def _bal_breaks(df):
    """Offset-independent running-balance check: the change in the statement
    balance between consecutive rows must equal (debit - credit) of that row.
    Returns (break_count, breaking_rows)."""
    if "BALANCE_NUM" not in df.columns or df["BALANCE_NUM"].isna().all():
        return 0, pd.DataFrame()
    d=df.sort_values("LINE_NO").copy()
    delta=d["DEBIT"]-d["CREDIT"]
    step=d["BALANCE_NUM"].diff()
    mism=step.notna()&((step-delta).abs()>1.0)
    return int(mism.sum()), d[mism]

def _tie_out(df_or_attrs_list,sum_deb,sum_cre):
    """Compare parsed totals to the statement footer totals. Accepts a df (uses
    its .attrs) or a list of dfs (sums their footers). Returns (ok, cd, cc)."""
    if isinstance(df_or_attrs_list,list):
        cd=np.nansum([t.attrs.get("ctrl_deb",np.nan) for t in df_or_attrs_list])
        cc=np.nansum([t.attrs.get("ctrl_cre",np.nan) for t in df_or_attrs_list])
    else:
        cd=df_or_attrs_list.attrs.get("ctrl_deb",np.nan)
        cc=df_or_attrs_list.attrs.get("ctrl_cre",np.nan)
    if np.isnan(cd) and np.isnan(cc): return None,cd,cc       # no footer found
    ok=(np.isnan(cd) or abs(sum_deb-cd)<1)and(np.isnan(cc) or abs(sum_cre-cc)<1)
    return ok,cd,cc

def _cheque_sequence(nums):
    """Outlier-aware cheque gap analysis. A real cheque book is a dense run, so
    isolated stragglers (>50 from any neighbour) are flagged as OUTLIER for
    review rather than inflating the 'missing' list. Returns
    (unique, duplicates, outliers, (lo,hi), missing)."""
    counts=Counter(nums); uniq=sorted(counts)
    dups=sorted(n for n,c in counts.items() if c>1)
    outliers=[]
    for i,v in enumerate(uniq):
        nb=[]
        if i>0: nb.append(v-uniq[i-1])
        if i<len(uniq)-1: nb.append(uniq[i+1]-v)
        if nb and min(nb)>50: outliers.append(v)
    core=[v for v in uniq if v not in outliers]
    missing=[]
    for i in range(len(core)-1):
        g=core[i+1]-core[i]
        if 1<g<=50: missing.extend(range(core[i]+1,core[i+1]))
    return uniq,dups,outliers,((core[0],core[-1]) if core else (None,None)),missing

def _style(ws,hdr_color):
    """Header styling, freeze, autofilter, borders, money/int formats, and
    row highlighting driven by a SEVERITY / STATUS / FLAG / FINDING column."""
    thin=Side(style="thin",color="FFDDDDDD"); bdr=Border(thin,thin,thin,thin)
    headers=[str(c.value).upper() if c.value is not None else "" for c in ws[1]]
    for c in ws[1]:
        c.fill=PatternFill("solid",fgColor=hdr_color)
        c.font=Font(bold=True,color=WHITE,size=9,name="Arial")
        c.alignment=Alignment(horizontal="center",vertical="center",wrap_text=True); c.border=bdr
    ws.row_dimensions[1].height=28; ws.freeze_panes="A2"
    if ws.max_column>=1:
        ws.auto_filter.ref=f"A1:{get_column_letter(ws.max_column)}{max(ws.max_row,1)}"
    sev_i=headers.index("SEVERITY") if "SEVERITY" in headers else -1
    sta_i=headers.index("STATUS") if "STATUS" in headers else -1
    flg_i=next((i for i,h in enumerate(headers) if h in ("FLAG","FINDING")),-1)
    altfill=PatternFill("solid",fgColor=ALT)
    for ri,row in enumerate(ws.iter_rows(min_row=2),start=2):
        rowfill=None
        if sev_i>=0:
            rowfill=SEV_FILL.get(str(row[sev_i].value).upper())
        elif sta_i>=0 and "MISSING" in str(row[sta_i].value).upper():
            rowfill=SEV_FILL["CRITICAL"]
        elif sta_i>=0 and ("DUPLICATE" in str(row[sta_i].value).upper()
                           or "OUTLIER" in str(row[sta_i].value).upper()):
            rowfill=SEV_FILL["HIGH"]
        elif flg_i>=0 and str(row[flg_i].value).strip():
            rowfill=SEV_FILL["MEDIUM"]
        for ci,cell in enumerate(row):
            cell.border=bdr; cell.font=Font(size=9,name="Arial")
            h=headers[ci] if ci<len(headers) else ""
            if isinstance(cell.value,(int,float)) and not isinstance(cell.value,bool):
                if any(k in h for k in MONEY_HINT): cell.number_format=MONEYFMT
                elif h in INT_COLS: cell.number_format=INTFMT
            if rowfill: cell.fill=PatternFill("solid",fgColor=rowfill)
            elif ri%2==0: cell.fill=altfill
    for col in ws.columns:
        ml=max((len(str(c.value or "")) for c in col),default=8)
        ws.column_dimensions[col[0].column_letter].width=min(max(ml+2,10),50)

def build_excel(branch,tills,treasury,journals,petty):
    buf=io.BytesIO(); sheets=[]; F=Findings()
    def add(nm,df,col=NAVY):
        if df is not None and len(df): sheets.append((nm[:31],_safe_df(df),col))
    def _pick(df,cols): return df[[c for c in cols if c in df.columns]]

    all_till=pd.concat(tills.values(),ignore_index=True) if tills else pd.DataFrame()
    p0=all_till["DATE"].min() if not all_till.empty and "DATE" in all_till else None
    p1=all_till["DATE"].max() if not all_till.empty and "DATE" in all_till else None

    # ===== TILLS =====
    WALL=["LINE_NO","DATE_FMT","TIME_DISPLAY","CATEGORY","MEMBER_NAME","OFFICER",
          "VOUCHER_NO","DEBIT","CREDIT","BALANCE_NUM","BALANCE_DISPLAY","REFERENCE","DETAILS"]
    REN={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME","BALANCE_NUM":"BALANCE_MWK",
         "BALANCE_DISPLAY":"BALANCE","CREDIT":"AMOUNT_OUT","DEBIT":"AMOUNT_IN"}
    for ti,(tname,tdf) in enumerate(tills.items(),start=10):
        lbl=re.sub(r"[^A-Z0-9]+","_",tname.upper()).strip("_")
        wd=tdf[tdf["CATEGORY"]=="WITHDRAWAL"]; dp=tdf[tdf["CATEGORY"]=="DEPOSIT"]
        dr=tdf[tdf["CATEGORY"]=="DIRECT_RECEIPT"]; cr=tdf[tdf["CATEGORY"]=="CASH_REQUEST"]
        sd,sc=tdf["DEBIT"].sum(),tdf["CREDIT"].sum()
        ok,cd,cc=_tie_out(tdf,sd,sc)
        if ok is False:
            F.add("HIGH",tname,"Parsed totals do not tie to statement footer",
                  "", f"DR diff {sd-cd:,.0f} / CR diff {sc-cc:,.0f}",
                  f"{ti}_{lbl}_SUMMARY","Re-export the statement; a row may be missing or duplicated")
        nb,nbrows=_bal_breaks(tdf)
        if nb:
            F.add("HIGH",tname,"Running balance does not reconcile line to line",
                  nb,"",f"{ti}_{lbl}_ALL","Inspect the flagged rows - a transaction may be missing or out of order")

        ts=[("Till",tname),("Period",f"{_fmt(tdf['DATE'].min())} to {_fmt(tdf['DATE'].max())}"),
            ("Total transactions",len(tdf)),
            ("Cash withdrawals (MWK)",wd["CREDIT"].sum()),("Withdrawal count",len(wd)),
            ("Cash deposits (MWK)",dp["DEBIT"].sum()),("Deposit count",len(dp)),
            ("Direct receipts - loan repayments (MWK)",dr["DEBIT"].sum()),
            ("Cash requests from main cashier (MWK)",cr["DEBIT"].sum()),
            ("Total debits parsed (MWK)",sd),("Total debits per statement (MWK)",cd),
            ("Total credits parsed (MWK)",sc),("Total credits per statement (MWK)",cc),
            ("Totals tie to statement?","Yes" if ok else ("No - investigate" if ok is False else "No footer found")),
            ("Running-balance breaks",nb)]
        add(f"{ti}_{lbl}_SUMMARY",pd.DataFrame(ts,columns=["Item","Value"]),GREEN)

        if not wd.empty:
            w=_pick(wd.sort_values("CREDIT",ascending=False),
                    ["MEMBER_NAME","CREDIT","DATE_FMT","TIME_DISPLAY","OFFICER","VOUCHER_NO",
                     "BALANCE_NUM","AFTER_HOURS","IS_WEEKEND","REFERENCE"])
            w=w.rename(columns={"MEMBER_NAME":"NAME","CREDIT":"AMOUNT_WITHDRAWN",
                                "DATE_FMT":"DATE","TIME_DISPLAY":"TIME","BALANCE_NUM":"BALANCE_MWK"})
            add(f"{ti}_{lbl}_WITHDRAWALS",w,GREEN)
        if not dp.empty:
            d=_pick(dp.sort_values("DEBIT",ascending=False),
                    ["MEMBER_NAME","DEBIT","DATE_FMT","TIME_DISPLAY","OFFICER","VOUCHER_NO","REFERENCE"])
            d=d.rename(columns={"MEMBER_NAME":"NAME","DEBIT":"AMOUNT_DEPOSITED",
                                "DATE_FMT":"DATE","TIME_DISPLAY":"TIME"})
            add(f"{ti}_{lbl}_DEPOSITS",d,TEAL)
        if not dr.empty:
            r=_pick(dr.sort_values("DEBIT",ascending=False),
                    ["MEMBER_NAME","DEBIT","DATE_FMT","TIME_DISPLAY","OFFICER","VOUCHER_NO","REFERENCE"])
            r=r.rename(columns={"MEMBER_NAME":"NAME","DEBIT":"AMOUNT_RECEIVED",
                                "DATE_FMT":"DATE","TIME_DISPLAY":"TIME"})
            add(f"{ti}_{lbl}_DIRECT_RECEIPTS",r,BLUE)

        daily=(tdf.groupby("DATE_FMT",sort=False).agg(
                DATE=("DATE","first"),
                WITHDRAWALS=("CATEGORY",lambda x:(x=="WITHDRAWAL").sum()),
                TOTAL_WITHDRAWN=("CREDIT",lambda x:x[tdf.loc[x.index,"CATEGORY"]=="WITHDRAWAL"].sum()),
                DEPOSITS=("CATEGORY",lambda x:(x=="DEPOSIT").sum()),
                TOTAL_DEPOSITED=("DEBIT",lambda x:x[tdf.loc[x.index,"CATEGORY"]=="DEPOSIT"].sum()))
               .reset_index(drop=True).sort_values("DATE"))
        daily["DATE"]=daily["DATE"].apply(_fmt)
        add(f"{ti}_{lbl}_DAILY",daily,NAVY)

        if not wd.empty:
            ms=(wd[wd["MEMBER_NAME"]!=""].groupby("MEMBER_NAME")
                .agg(WITHDRAWALS=("CREDIT","count"),TOTAL_WITHDRAWN=("CREDIT","sum"),
                     LARGEST_SINGLE=("CREDIT","max")).reset_index()
                .sort_values("TOTAL_WITHDRAWN",ascending=False)
                .rename(columns={"MEMBER_NAME":"NAME"}))
            add(f"{ti}_{lbl}_MEMBERS",ms,PURPLE)

        allt=_pick(tdf.sort_values("LINE_NO"),WALL).rename(columns=REN)
        add(f"{ti}_{lbl}_ALL",allt,NAVY)

        # ---- real-logic anomalies for this till ----
        an=[]
        # duplicate payout: same member + amount + date (possible double pay)
        dm=wd.duplicated(subset=["DATE_FMT","MEMBER_NAME","CREDIT"],keep=False)&(wd["MEMBER_NAME"]!="")&(wd["CREDIT"]>0)
        if dm.any():
            a=_pick(wd[dm],["LINE_NO","DATE_FMT","TIME_DISPLAY","MEMBER_NAME","CREDIT","OFFICER"]).copy()
            a["FLAG"]="Duplicate: same member, amount & date"; an.append(a)
            F.add("HIGH",tname,"Possible duplicate withdrawals (same member, amount, date)",
                  int(dm.sum()),wd[dm]["CREDIT"].sum(),f"{ti}_{lbl}_FLAGS","Confirm each against the physical voucher; rule out double payment")
        # cash withdrawal with no member name (weak audit trail)
        nn=wd[wd["MEMBER_NAME"]==""]
        if not nn.empty:
            a=_pick(nn,["LINE_NO","DATE_FMT","TIME_DISPLAY","CREDIT","OFFICER"]).copy()
            a["FLAG"]="Withdrawal with no member name captured"; an.append(a)
            F.add("MEDIUM",tname,"Withdrawals with no member name",
                  len(nn),nn["CREDIT"].sum(),f"{ti}_{lbl}_FLAGS","Confirm payee identity on the slip")
        # statistical large-value outlier (adapts to this till's own distribution)
        # genuinely extreme value: top 1% for this till (a short, actionable list,
        # not the whole legitimate loan-disbursement cluster an IQR rule would catch)
        wv=wd["CREDIT"]
        if len(wv)>=20:
            thr=wv.quantile(0.99)
            big=wd[wd["CREDIT"]>thr]
            if not big.empty:
                a=_pick(big,["LINE_NO","DATE_FMT","MEMBER_NAME","CREDIT","OFFICER"]).copy()
                a["FLAG"]=f"Top 1% largest (> MWK {thr:,.0f})"; an.append(a)
                F.add("MEDIUM",tname,"Largest-value withdrawals (top 1%) - verify authorisation",
                      len(big),big["CREDIT"].sum(),f"{ti}_{lbl}_FLAGS","Confirm dual authorisation / limits were observed")
        # after-hours (low) - kept as information, not noise
        ah=tdf[tdf.get("AFTER_HOURS",False)==True]
        if not ah.empty:
            a=_pick(ah,["LINE_NO","DATE_FMT","TIME_DISPLAY","CATEGORY","MEMBER_NAME","CREDIT","DEBIT"]).copy()
            a["FLAG"]="Outside normal hours (before 07:00 / after 19:00)"; an.append(a)
            F.add("LOW",tname,"Transactions outside normal hours",len(ah),"",
                  f"{ti}_{lbl}_FLAGS","Confirm these were legitimate end/early-day entries")
        if an:
            cols=["LINE_NO","DATE_FMT","TIME_DISPLAY","CATEGORY","MEMBER_NAME","CREDIT","DEBIT","OFFICER","FLAG"]
            flags=pd.concat([x.reindex(columns=[c for c in cols if c in x.columns or c=="FLAG"]) for x in an],ignore_index=True)
            flags=flags.rename(columns={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME","CREDIT":"AMOUNT_OUT","DEBIT":"AMOUNT_IN"})
            add(f"{ti}_{lbl}_FLAGS",flags,RED)

    # ===== TREASURY =====
    if treasury is not None and not treasury.empty:
        TR=["LINE_NO","DATE_FMT","TIME_DISPLAY","CHEQUE_NO","TELLER_NO","ACTIVITY",
            "OFFICER","DEBIT","CREDIT","BALANCE_NUM","BALANCE","REFERENCE"]
        TREN={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME","BALANCE_NUM":"BALANCE_MWK"}
        sd,sc=treasury["DEBIT"].sum(),treasury["CREDIT"].sum()
        ok,cd,cc=_tie_out(treasury,sd,sc)
        if ok is False:
            F.add("HIGH","Treasury","Parsed totals do not tie to statement footer","",
                  f"DR diff {sd-cd:,.0f} / CR diff {sc-cc:,.0f}","30_TREASURY_CATEGORY","Re-export; a row may be missing/duplicated")
        nb,_=_bal_breaks(treasury)
        if nb: F.add("HIGH","Treasury","Running balance does not reconcile line to line",nb,"","32_CHEQUE_SEQUENCE","Inspect ordering / missing rows")

        cat=(treasury.groupby("CATEGORY").agg(ROW_COUNT=("DEBIT","count"),
              TOTAL_DEBIT=("DEBIT","sum"),TOTAL_CREDIT=("CREDIT","sum")).reset_index())
        cat["NET"]=cat["TOTAL_DEBIT"]-cat["TOTAL_CREDIT"]
        add("30_TREASURY_CATEGORY",cat.sort_values("TOTAL_DEBIT",ascending=False),NAVY)

        cfb=treasury[treasury["CATEGORY"]=="CASH_FROM_BANK"].copy()
        if not cfb.empty:
            cfb["CHEQUE_INT"]=pd.to_numeric(cfb["CHEQUE_NO"],errors="coerce")
            reg=_pick(cfb.sort_values("CHEQUE_INT"),
                      ["CHEQUE_NO","DATE_FMT","DEBIT","DEPOSITOR","OFFICER","BATCH_NO","REFERENCE"])
            reg=reg.rename(columns={"DATE_FMT":"DATE_DRAWN","DEBIT":"AMOUNT_DRAWN"})
            reg["VERIFIED_Y_N"]=""           # manual tick during physical recon
            reg["CHEQUE_NO"]=pd.to_numeric(reg["CHEQUE_NO"],errors="coerce").astype("Int64")
            add("31_CHEQUE_REGISTER",reg,GREEN)

            nums=cfb["CHEQUE_INT"].dropna().astype(int).tolist()
            if len(nums)>1:
                uniq,dups,outl,(lo,hi),miss=_cheque_sequence(nums)
                seq=[]
                byno={}
                for _,rr in cfb.dropna(subset=["CHEQUE_INT"]).iterrows():
                    byno.setdefault(int(rr["CHEQUE_INT"]),(rr["DATE_FMT"],rr["DEBIT"]))
                for n in range(lo,hi+1):
                    if n in miss: seq.append([n,"*** MISSING ***","",""])
                    elif n in dups: seq.append([n,f"DUPLICATE x{Counter(nums)[n]}",byno.get(n,('',''))[0],byno.get(n,('',0))[1]])
                    elif n in byno: seq.append([n,"found",byno[n][0],byno[n][1]])
                    else: seq.append([n,"*** MISSING ***","",""])
                for n in outl:
                    seq.append([n,"OUTLIER (review)",byno.get(n,('',''))[0],byno.get(n,('',0))[1]])
                add("32_CHEQUE_SEQUENCE",pd.DataFrame(seq,
                    columns=["CHEQUE_NO","STATUS","DATE_DRAWN","AMOUNT_DRAWN"]),GREEN)
                if miss:
                    F.add("HIGH","Treasury","Cheque numbers missing within the issued sequence",
                          len(miss),"","32_CHEQUE_SEQUENCE",
                          f"Verify against the physical cheque book: {miss[:25]}{'...' if len(miss)>25 else ''}")
                if dups:
                    F.add("HIGH","Treasury","Cheque numbers appearing more than once",
                          len(dups),"","32_CHEQUE_SEQUENCE",
                          f"Confirm not double-drawn: {dups[:25]}")
                if outl:
                    F.add("MEDIUM","Treasury","Isolated cheque number(s) far outside the sequence",
                          len(outl),"","32_CHEQUE_SEQUENCE",f"Likely a different book or typo: {outl}")

        for cat_,nm,col in [("CASH_FROM_BANK","33_CASH_FROM_BANK",GREEN),
                            ("CASH_TO_BANK","34_CASH_TO_BANK",BLUE),
                            ("CASH_TO_TELLERS","35_CASH_TO_TELLERS",TEAL),
                            ("CASH_FROM_TELLERS","36_CASH_FROM_TELLERS",TEAL),
                            ("PETTY_CASH_TOPUP","37_PETTY_TOPUP",PURPLE),
                            ("CASH_DIFFERENCE","38_CASH_DIFFERENCES",AMBER),
                            ("REVERSAL","39_TREASURY_REVERSALS",RED),
                            ("OTHER","40_TREASURY_OTHER",NAVY)]:
            sub=treasury[treasury["CATEGORY"]==cat_]
            if not sub.empty:
                add(nm,_pick(sub.sort_values("LINE_NO"),TR).rename(columns=TREN),col)
        rv=treasury[treasury["CATEGORY"]=="REVERSAL"]
        if not rv.empty:
            F.add("MEDIUM","Treasury","Reversals present",len(rv),rv["CREDIT"].sum(),
                  "39_TREASURY_REVERSALS","Confirm each reversal has an approved original entry")
        cdf=treasury[treasury["CATEGORY"]=="CASH_DIFFERENCE"]
        if not cdf.empty:
            F.add("HIGH","Treasury","Cash overage / shortage entries",len(cdf),
                  (cdf["DEBIT"]+cdf["CREDIT"]).sum(),"38_CASH_DIFFERENCES","Investigate and document the cause of each difference")

    # ===== JOURNALS =====
    if journals is not None and not journals.empty:
        JC=["BATCH_NO","DATE_FMT","WEEKDAY","DESC_CLEAN","CATEGORY","CREATED_BY",
            "APPROVED_BY","DR_NAME","CR_NAME","DEBIT","CREDIT"]
        def jout(df): return _pick(df,JC).rename(columns={"DATE_FMT":"DATE"})
        add("50_JOURNALS_ALL",jout(journals.sort_values("DATE")),BLUE)
        sm=int(journals["SAME_MAKER_CHECKER"].sum()); nc=int(journals["NO_CHECKER"].sum())
        p2=int(journals["PERSON_TO_PERSON"].sum()); sp=int(journals["SAME_PERSON_DR_CR"].sum())
        if sm: F.add("CRITICAL","Journals","Same person created and approved the journal (maker-checker failure)",sm,journals[journals['SAME_MAKER_CHECKER']]["DEBIT"].sum(),"51_JOURNALS_INVESTIGATE","Enforce segregation of duties; review each batch")
        if nc: F.add("CRITICAL","Journals","Journals with no checker/approver recorded",nc,journals[journals['NO_CHECKER']]["DEBIT"].sum(),"51_JOURNALS_INVESTIGATE","Confirm authorisation; require an approver on all journals")
        if p2: F.add("HIGH","Journals","Person-to-person transfers (neither side a system account)",p2,"","51_JOURNALS_INVESTIGATE","Verify legitimacy of each transfer")
        if sp: F.add("HIGH","Journals","Same person on both debit and credit side",sp,"","51_JOURNALS_INVESTIGATE","Investigate for circular / suspicious entries")
        inv=[]
        for sub,msg,sev in [
            (journals[journals["SAME_MAKER_CHECKER"]==True],"Same maker and checker - control failure","CRITICAL"),
            (journals[journals["NO_CHECKER"]==True],"No approver recorded","CRITICAL"),
            (journals[journals["PERSON_TO_PERSON"]==True],"Person-to-person transfer - verify","HIGH"),
            (journals[journals["SAME_PERSON_DR_CR"]==True],"Same person debit and credit - investigate","HIGH")]:
            for _,r in sub.iterrows():
                d={c:r[c] for c in JC if c in r.index}; d["SEVERITY"]=sev; d["FINDING"]=msg; inv.append(d)
        if inv:
            idf=pd.DataFrame(inv).rename(columns={"DATE_FMT":"DATE"})
            front=["SEVERITY","FINDING"]+[c for c in idf.columns if c not in ("SEVERITY","FINDING")]
            add("51_JOURNALS_INVESTIGATE",idf[front],RED)
        add("52_J_CREATORS",journals.groupby("CREATED_BY").agg(BATCHES=("BATCH_NO","nunique"),TOTAL_DEBIT=("DEBIT","sum")).reset_index().sort_values("TOTAL_DEBIT",ascending=False),NAVY)
        ap=journals[journals["APPROVED_BY"].str.strip()!=""]
        if not ap.empty:
            add("53_J_APPROVERS",ap.groupby("APPROVED_BY").agg(APPROVED=("BATCH_NO","nunique")).reset_index().sort_values("APPROVED",ascending=False),NAVY)

    # ===== PETTY =====
    if petty is not None and not petty.empty:
        sd,sc=petty["DEBIT"].sum(),petty["CREDIT"].sum()
        ok,cd,cc=_tie_out(petty,sd,sc)
        if ok is False:
            F.add("HIGH","Petty cash","Parsed totals do not tie to statement footer","",
                  f"DR diff {sd-cd:,.0f} / CR diff {sc-cc:,.0f}","70_PETTY_REGISTER","Re-export; check for missing rows")
        pc=["LINE_NO","DATE_FMT","TIME_DISPLAY","BATCH_NO","CATEGORY","ACTIVITY",
            "OFFICER","DEBIT","CREDIT","BALANCE_NUM","FLAG"]
        reg=_pick(petty.sort_values("LINE_NO"),pc).rename(columns={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME","BALANCE_NUM":"BALANCE_MWK"})
        reg["VERIFIED_Y_N"]=""
        add("70_PETTY_REGISTER",reg,TEAL)
        cs=(petty[petty["CREDIT"]>0].groupby("CATEGORY").agg(TRANSACTIONS=("CREDIT","count"),
             TOTAL_SPENT=("CREDIT","sum")).reset_index().sort_values("TOTAL_SPENT",ascending=False))
        if not cs.empty:
            cs["PCT_OF_TOTAL"]=(cs["TOTAL_SPENT"]/cs["TOTAL_SPENT"].sum()*100).round(1)
        add("71_PETTY_CATEGORY",cs,NAVY)
        flagged=petty[petty["FLAG"]!=""]
        if not flagged.empty:
            a=_pick(flagged,["LINE_NO","DATE_FMT","CATEGORY","ACTIVITY","OFFICER","CREDIT","FLAG"]).rename(columns={"DATE_FMT":"DATE","CREDIT":"AMOUNT"})
            add("72_PETTY_FLAGS",a,RED)
            F.add("MEDIUM","Petty cash","Petty cash items flagged for review",len(flagged),
                  flagged["CREDIT"].sum(),"72_PETTY_FLAGS","Match each flagged item to its voucher and receipt")

    # ===== OVERVIEW (built last so it can summarise everything) =====
    ov=[("BRANCH SUPERVISION ANALYSIS",""),("Branch",branch or "Not specified"),
        ("Period",f"{_fmt(p0)} to {_fmt(p1)}" if p0 is not None else "N/A"),
        ("Generated",datetime.now().strftime("%d/%m/%Y %H:%M")),
        ("Automated findings raised",len([i for i in F.items])),("","")]
    if not all_till.empty:
        wd=all_till[all_till["CATEGORY"]=="WITHDRAWAL"]; dp=all_till[all_till["CATEGORY"]=="DEPOSIT"]
        dr=all_till[all_till["CATEGORY"]=="DIRECT_RECEIPT"]
        tok,tcd,tcc=_tie_out(list(tills.values()),all_till["DEBIT"].sum(),all_till["CREDIT"].sum())
        ov+=[("TILLS",""),("Number of tills",len(tills)),("Transactions (all tills)",len(all_till)),
             ("Total withdrawals (MWK)",_money_str(wd["CREDIT"].sum())),("Withdrawal count",len(wd)),
             ("Total deposits (MWK)",_money_str(dp["DEBIT"].sum())),
             ("Total direct receipts (MWK)",_money_str(dr["DEBIT"].sum())),
             ("Tills tie to statements?","Yes" if tok else ("No - investigate" if tok is False else "No footer found")),("","")]
    if treasury is not None and not treasury.empty:
        cfb=treasury[treasury["CATEGORY"]=="CASH_FROM_BANK"]; ctt=treasury[treasury["CATEGORY"]=="CASH_TO_TELLERS"]
        ov+=[("TREASURY",""),("Cash received from bank (MWK)",_money_str(cfb["DEBIT"].sum())),
             ("Cheque withdrawals",len(cfb)),("Cash issued to tellers (MWK)",_money_str(ctt["CREDIT"].sum())),("","")]
    if journals is not None and not journals.empty:
        ov+=[("JOURNALS",""),("Journal batches",journals["BATCH_NO"].nunique()),
             ("Maker-checker failures",int(journals["SAME_MAKER_CHECKER"].sum())),
             ("No approver recorded",int(journals["NO_CHECKER"].sum())),("","")]
    if petty is not None and not petty.empty:
        ov+=[("PETTY CASH",""),("Transactions",len(petty)),
             ("Total expenditure (MWK)",_money_str(petty["CREDIT"].sum())),
             ("Items flagged",int((petty["FLAG"]!="").sum()))]
    sheets.insert(0,("01_OVERVIEW",_safe_df(pd.DataFrame(ov,columns=["Item","Value"])),NAVY))
    sheets.insert(0,("00_FINDINGS",_safe_df(F.df()),RED))

    # write + style
    with pd.ExcelWriter(buf,engine="openpyxl") as writer:
        for nm,df,_ in sheets: df.to_excel(writer,sheet_name=nm,index=False)
    buf.seek(0); wb=load_workbook(buf)
    for nm,_,col in sheets:
        if nm in wb.sheetnames: _style(wb[nm],col)
    out=io.BytesIO(); wb.save(out); out.seek(0); return out.read()

# ── sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Branch Supervision")
    st.markdown("**Data Analysis Tool**")
    st.divider()
    page=st.radio("Navigate",["Upload & Clean","Till Analysis","Treasury",
                               "Journals & Fraud","Petty Cash","Export Report"],
                  label_visibility="collapsed")
    st.divider()
    st.markdown("**Session**")
    st.markdown(f"Branch: `{st.session_state.branch or 'not set'}`")
    st.markdown(f"Tills: {len(st.session_state.tills)} loaded")
    st.markdown(f"Treasury: {'yes' if st.session_state.treasury is not None else 'no'}")
    jdf=st.session_state.journals
    st.markdown(f"Journals: {'yes' if jdf is not None and not jdf.empty else 'no'}")
    st.markdown(f"Petty: {'yes' if st.session_state.petty is not None else 'no'}")
    st.divider()
    if st.button("Reset session",use_container_width=True):
        for k in list(st.session_state.keys()): del st.session_state[k]
        st.rerun()
    st.caption("Risk & Compliance Dept")

# ── pages ──────────────────────────────────────────────────────────────────────
if page=="Upload & Clean":
    st.title("Upload & Clean Branch Files")
    st.caption("Column detection is automatic — works with any branch.")
    cb,_=st.columns([1,3])
    with cb:
        b=st.text_input("Branch name",value=st.session_state.branch,placeholder="e.g. Mzimba")
        if b: st.session_state.branch=b
    st.divider()
    c1,c2,c3,c4=st.columns(4)
    with c1:
        st.subheader("Till files")
        tfs=st.file_uploader("Till files",type=["xls","xlsx"],accept_multiple_files=True,label_visibility="collapsed")
        if tfs:
            for f in tfs:
                ln=f.name.lower(); m=re.search(r"till[_\s]*(\d+)",ln)
                lbl=f"Till {m.group(1)}" if m else f.name.split(".")[0].title()
                with st.spinner(f"Cleaning {f.name}..."):
                    st.session_state.tills[lbl]=clean_till(f.read(),f.name,lbl)
            st.success(f"{len(st.session_state.tills)} till(s) loaded.")
    with c2:
        st.subheader("Treasury file")
        tf=st.file_uploader("Treasury",type=["xls","xlsx"],label_visibility="collapsed")
        if tf:
            with st.spinner("Cleaning..."):
                st.session_state.treasury=clean_treasury(tf.read(),tf.name)
            st.success(f"Treasury: {len(st.session_state.treasury):,} rows")
    with c3:
        st.subheader("Journal file")
        jf=st.file_uploader("Journals",type=["xls","xlsx"],label_visibility="collapsed")
        if jf:
            with st.spinner("Parsing..."):
                j=parse_journals(jf.read(),jf.name); st.session_state.journals=j
            if j is not None and not j.empty:
                st.success(f"Journals: {j['BATCH_NO'].nunique():,} batches")
            else: st.error("Could not parse — check format.")
    with c4:
        st.subheader("Petty cash file")
        pf=st.file_uploader("Petty",type=["xls","xlsx"],label_visibility="collapsed")
        if pf:
            with st.spinner("Cleaning..."):
                st.session_state.petty=clean_petty(pf.read(),pf.name)
            st.success(f"Petty: {len(st.session_state.petty):,} rows")
    if st.session_state.tills or st.session_state.treasury is not None:
        st.divider(); st.info("Files loaded. Use the sidebar to navigate.")

elif page=="Till Analysis":
    st.title("Till Transaction Analysis")
    tills=st.session_state.tills
    if not tills: st.warning("No till files loaded."); st.stop()
    all_till=pd.concat(tills.values(),ignore_index=True)
    wd=all_till[all_till["CATEGORY"]=="WITHDRAWAL"]; dp=all_till[all_till["CATEGORY"]=="DEPOSIT"]
    dr=all_till[all_till["CATEGORY"]=="DIRECT_RECEIPT"]
    ah=int(all_till.get("AFTER_HOURS",pd.Series(False)).sum()) if "AFTER_HOURS" in all_till else 0
    wk=int(all_till.get("IS_WEEKEND",pd.Series(False)).sum()) if "IS_WEEKEND" in all_till else 0
    c1,c2,c3,c4,c5=st.columns(5)
    with c1: st.metric("Transactions",f"{len(all_till):,}")
    with c2: st.metric("Withdrawals (MWK)",f"{wd['CREDIT'].sum():,.0f}")
    with c3: st.metric("Deposits (MWK)",f"{dp['DEBIT'].sum():,.0f}")
    with c4: st.metric("Direct receipts (MWK)",f"{dr['DEBIT'].sum():,.0f}")
    with c5: st.metric("After-hours + Weekend",f"{ah+wk}",delta="Investigate" if ah+wk>0 else None,delta_color="inverse")
    st.divider()
    sel=st.selectbox("View",["All tills combined"]+list(tills.keys()))
    vdf=tills[sel] if sel!="All tills combined" else all_till
    t1,t2,t3,t4,t5=st.tabs(["Withdrawals","Deposits","Direct Receipts","Member Summary","Anomalies"])
    with t1:
        w=vdf[vdf["CATEGORY"]=="WITHDRAWAL"]
        st.caption(f"{len(w):,} withdrawals — MWK {w['CREDIT'].sum():,.0f}")
        if not w.empty:
            show_df(w[[c for c in ["LINE_NO","DATE_FMT","TIME_DISPLAY","MEMBER_NAME","VOUCHER_NO","CREDIT","AFTER_HOURS","IS_WEEKEND","BALANCE_DISPLAY"] if c in w.columns]].rename(columns={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME","CREDIT":"AMOUNT_WITHDRAWN","BALANCE_DISPLAY":"BALANCE"}))
        if "DATE" in w.columns and not w.empty:
            d2=w.groupby(w["DATE"].dt.date)["CREDIT"].sum().reset_index(); d2.columns=["Date","Amount"]
            fig=px.bar(d2,x="Date",y="Amount",title="Daily total withdrawals (MWK)",height=260)
            fig.update_layout(showlegend=False); st.plotly_chart(fig,use_container_width=True)
    with t2:
        d=vdf[vdf["CATEGORY"]=="DEPOSIT"]
        st.caption(f"{len(d):,} deposits — MWK {d['DEBIT'].sum():,.0f}")
        if not d.empty:
            show_df(d[[c for c in ["LINE_NO","DATE_FMT","TIME_DISPLAY","MEMBER_NAME","VOUCHER_NO","DEBIT","BALANCE_DISPLAY"] if c in d.columns]].rename(columns={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME","DEBIT":"AMOUNT_DEPOSITED","BALANCE_DISPLAY":"BALANCE"}))
    with t3:
        r=vdf[vdf["CATEGORY"]=="DIRECT_RECEIPT"]
        st.caption(f"{len(r):,} direct receipts — MWK {r['DEBIT'].sum():,.0f}")
        if not r.empty:
            show_df(r[[c for c in ["LINE_NO","DATE_FMT","TIME_DISPLAY","MEMBER_NAME","VOUCHER_NO","DEBIT","BALANCE_DISPLAY"] if c in r.columns]].rename(columns={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME","DEBIT":"AMOUNT_RECEIVED","BALANCE_DISPLAY":"BALANCE"}))
    with t4:
        w2=vdf[vdf["CATEGORY"]=="WITHDRAWAL"]
        if "MEMBER_NAME" in w2.columns and not w2.empty:
            ms=(w2[w2["MEMBER_NAME"]!=""].groupby("MEMBER_NAME").agg(WITHDRAWALS=("CREDIT","count"),TOTAL_WITHDRAWN=("CREDIT","sum"),LARGEST_SINGLE=("CREDIT","max")).reset_index().sort_values("TOTAL_WITHDRAWN",ascending=False))
            show_df(ms.head(50),height=500)
            fig=px.bar(ms.head(15),x="MEMBER_NAME",y="TOTAL_WITHDRAWN",title="Top 15 members (MWK)",height=280)
            fig.update_layout(xaxis_tickangle=-40); st.plotly_chart(fig,use_container_width=True)
    with t5:
        ah_df=vdf[vdf.get("AFTER_HOURS",pd.Series(False,index=vdf.index))==True] if "AFTER_HOURS" in vdf else pd.DataFrame()
        wk_df=vdf[vdf.get("IS_WEEKEND",pd.Series(False,index=vdf.index))==True] if "IS_WEEKEND" in vdf else pd.DataFrame()
        if not ah_df.empty:
            flag(f"{len(ah_df)} after-hours transaction(s).","high")
            show_df(ah_df[[c for c in ["LINE_NO","DATE_FMT","TIME_DISPLAY","CATEGORY","MEMBER_NAME","CREDIT","DEBIT"] if c in ah_df.columns]].rename(columns={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME"}),height=250)
        if not wk_df.empty:
            flag(f"{len(wk_df)} weekend transaction(s).","high")
            show_df(wk_df[[c for c in ["LINE_NO","DATE_FMT","WEEKDAY","CATEGORY","MEMBER_NAME","CREDIT","DEBIT"] if c in wk_df.columns]].rename(columns={"DATE_FMT":"DATE"}),height=250)
        if ah_df.empty and wk_df.empty: st.success("No anomalies.")

elif page=="Treasury":
    st.title("Treasury Analysis")
    tr=st.session_state.treasury
    if tr is None: st.warning("No treasury file loaded."); st.stop()
    cfb=tr[tr["CATEGORY"]=="CASH_FROM_BANK"]; ctb=tr[tr["CATEGORY"]=="CASH_TO_BANK"]
    ctt=tr[tr["CATEGORY"]=="CASH_TO_TELLERS"]; cft=tr[tr["CATEGORY"]=="CASH_FROM_TELLERS"]
    c1,c2,c3,c4=st.columns(4)
    with c1: st.metric("Cash from bank",f"MWK {cfb['DEBIT'].sum():,.0f}")
    with c2: st.metric("Cash to bank",f"MWK {ctb['CREDIT'].sum():,.0f}")
    with c3: st.metric("Issued to tellers",f"MWK {ctt['CREDIT'].sum():,.0f}")
    with c4: st.metric("Received from tellers",f"MWK {cft['DEBIT'].sum():,.0f}")
    st.divider()
    t1,t2,t3=st.tabs(["Cheque Register","By Category","All Transactions"])
    with t1:
        if not cfb.empty:
            chq=cfb[["DATE_FMT","CHEQUE_NO","BATCH_NO","DEBIT","ACTIVITY"]].rename(columns={"DATE_FMT":"DATE","DEBIT":"AMOUNT_DRAWN","ACTIVITY":"DESCRIPTION"}); chq["VERIFIED"]=""
            show_df(chq.sort_values("CHEQUE_NO"),height=500)
            nums=pd.to_numeric(chq["CHEQUE_NO"],errors="coerce").dropna().astype(int)
            if len(nums)>1:
                miss=sorted(set(range(nums.min(),nums.max()+1))-set(nums))
                if miss: flag(f"Cheque gaps: {miss[:20]}{'...' if len(miss)>20 else ''}. Verify against physical cheque book.","high")
        else: st.info("No CASH_FROM_BANK entries found.")
    with t2:
        cat=st.selectbox("Category",sorted(tr["CATEGORY"].unique()))
        sub=tr[tr["CATEGORY"]==cat]
        show_df(sub[[c for c in ["LINE_NO","DATE_FMT","TIME_DISPLAY","CHEQUE_NO","ACTIVITY","DEBIT","CREDIT","BALANCE","REFERENCE"] if c in sub.columns]].rename(columns={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME"}))
    with t3:
        show_df(tr[[c for c in ["LINE_NO","DATE_FMT","TIME_DISPLAY","CATEGORY","CHEQUE_NO","ACTIVITY","DEBIT","CREDIT","BALANCE"] if c in tr.columns]].rename(columns={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME"}))

elif page=="Journals & Fraud":
    st.title("Journal Analysis & Fraud Detection")
    jdf=st.session_state.journals
    if jdf is None or jdf.empty: st.warning("No journal file loaded."); st.stop()
    sm=int(jdf["SAME_MAKER_CHECKER"].sum()); nc=int(jdf["NO_CHECKER"].sum())
    p2=int(jdf["PERSON_TO_PERSON"].sum()); sd=int(jdf["SAME_PERSON_DR_CR"].sum()); wk=int(jdf["IS_WEEKEND"].sum())
    c1,c2,c3,c4,c5=st.columns(5)
    with c1: st.metric("Batches",f"{jdf['BATCH_NO'].nunique():,}")
    with c2: st.metric("Same maker/checker",f"{sm}",delta="CRITICAL" if sm>0 else None,delta_color="inverse")
    with c3: st.metric("No checker",f"{nc}",delta="CRITICAL" if nc>0 else None,delta_color="inverse")
    with c4: st.metric("Person-to-person",f"{p2}",delta="Investigate" if p2>0 else None,delta_color="inverse")
    with c5: st.metric("Weekend journals",f"{wk}",delta="Review" if wk>0 else None,delta_color="inverse")
    st.divider()
    JD=["BATCH_NO","DATE_FMT","WEEKDAY","DESC_CLEAN","CATEGORY","CREATED_BY","APPROVED_BY","DR_NAME","CR_NAME","DEBIT","CREDIT"]
    def jshow(df): show_df(_pick(df,[c for c in JD if c in df.columns]).rename(columns={"DATE_FMT":"DATE"}))
    def _pick(df,cols): return df[[c for c in cols if c in df.columns]]
    t1,t2,t3,t4=st.tabs(["All journals","Fraud & Investigate","By category","By person"])
    with t1: jshow(jdf.sort_values("DATE"))
    with t2:
        if sm>0:
            flag(f"MAKER-CHECKER VIOLATION: {sm} leg(s). MWK {jdf[jdf['SAME_MAKER_CHECKER']==True]['DEBIT'].sum():,.0f}","critical")
            jshow(jdf[jdf["SAME_MAKER_CHECKER"]==True].drop_duplicates("BATCH_NO")); st.divider()
        if nc>0:
            flag(f"NO APPROVER: {nc} leg(s) have no checker recorded.","critical")
            jshow(jdf[jdf["NO_CHECKER"]==True].drop_duplicates("BATCH_NO")); st.divider()
        if p2>0:
            flag(f"PERSON-TO-PERSON: {p2} leg(s) transfer between individuals. Verify each.","high")
            jshow(jdf[jdf["PERSON_TO_PERSON"]==True]); st.divider()
        if sd>0:
            flag(f"SAME PERSON DR & CR: {sd} leg(s). Investigate.","high")
            jshow(jdf[jdf["SAME_PERSON_DR_CR"]==True]); st.divider()
        if wk>0:
            flag(f"WEEKEND JOURNALS: {wk} entries.","high")
            jshow(jdf[jdf["IS_WEEKEND"]==True].drop_duplicates("BATCH_NO"))
        if sm==0 and nc==0 and p2==0 and sd==0 and wk==0: st.success("No fraud flags.")
    with t3:
        cat=(jdf[jdf["DEBIT"]>0].groupby("CATEGORY").agg(BATCHES=("BATCH_NO","nunique"),TOTAL_MWK=("DEBIT","sum")).reset_index().sort_values("TOTAL_MWK",ascending=False))
        c1,c2=st.columns([1,2])
        with c1: show_df(cat,height=350)
        with c2:
            fig=px.pie(cat,values="TOTAL_MWK",names="CATEGORY",hole=0.45,title="Journal amounts by category",height=300)
            st.plotly_chart(fig,use_container_width=True)
        sc=st.selectbox("Drill into category",sorted(jdf["CATEGORY"].unique()))
        jshow(jdf[jdf["CATEGORY"]==sc].sort_values("DATE"))
    with t4:
        cc,ca=st.columns(2)
        with cc:
            st.caption("Created by")
            show_df(jdf.groupby("CREATED_BY").agg(BATCHES=("BATCH_NO","nunique"),TOTAL_DEBIT=("DEBIT","sum")).reset_index().sort_values("TOTAL_DEBIT",ascending=False),height=300)
        with ca:
            st.caption("Approved by")
            ap=jdf[jdf["APPROVED_BY"].str.strip()!=""]
            show_df(ap.groupby("APPROVED_BY").agg(APPROVED=("BATCH_NO","nunique")).reset_index().sort_values("APPROVED",ascending=False),height=300)

elif page=="Petty Cash":
    st.title("Petty Cash Analysis")
    pdf=st.session_state.petty
    if pdf is None or pdf.empty: st.warning("No petty cash file loaded."); st.stop()
    c1,c2,c3=st.columns(3)
    with c1: st.metric("Transactions",f"{len(pdf):,}")
    with c2: st.metric("Total expenditure (MWK)",f"{pdf['CREDIT'].sum():,.0f}")
    with c3: st.metric("Anomaly flags",f"{int((pdf['FLAG']!='').sum())}",delta="Review" if (pdf['FLAG']!='').sum()>0 else None,delta_color="inverse")
    st.divider()
    pt1,pt2,pt3=st.tabs(["Transaction register","Category summary","Anomalies"])
    with pt1:
        show_df(pdf[[c for c in ["LINE_NO","DATE_FMT","TIME_DISPLAY","CATEGORY","ACTIVITY","DEBIT","CREDIT","BALANCE","FLAG"] if c in pdf.columns]].rename(columns={"DATE_FMT":"DATE","TIME_DISPLAY":"TIME"}))
    with pt2:
        cs=(pdf[pdf["CREDIT"]>0].groupby("CATEGORY").agg(TRANSACTIONS=("CREDIT","count"),TOTAL_SPENT=("CREDIT","sum")).reset_index().sort_values("TOTAL_SPENT",ascending=False))
        tot=cs["TOTAL_SPENT"].sum(); cs["PCT"]=(cs["TOTAL_SPENT"]/tot*100).round(1)
        c1,c2=st.columns([1,2])
        with c1: show_df(cs,height=300)
        with c2:
            fig=px.pie(cs,values="TOTAL_SPENT",names="CATEGORY",hole=0.45,title="Petty cash by category",height=280)
            st.plotly_chart(fig,use_container_width=True)
    with pt3:
        ap=pdf[pdf["FLAG"]!=""]
        if ap.empty: st.success("No anomalies.")
        else: show_df(ap[[c for c in ["LINE_NO","DATE_FMT","ACTIVITY","CREDIT","FLAG"] if c in ap.columns]].rename(columns={"DATE_FMT":"DATE","CREDIT":"AMOUNT"}))

elif page=="Export Report":
    st.title("Export Full Analysis Report")
    tills=st.session_state.tills; treasury=st.session_state.treasury
    journals=st.session_state.journals; petty=st.session_state.petty
    branch=st.session_state.branch or "BRANCH"
    if not tills and treasury is None and journals is None and petty is None:
        st.warning("No data loaded."); st.stop()
    if st.button("Generate workbook",type="primary"):
        with st.spinner("Building workbook..."):
            xl=build_excel(branch,tills,treasury,journals,petty)
            fname=f"SUPERVISION_{branch.upper().replace(' ','_')}_{datetime.now().strftime('%Y%m%d')}.xlsx"
            st.download_button("Download Excel Workbook",data=xl,file_name=fname,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.success(f"Ready: {fname}")
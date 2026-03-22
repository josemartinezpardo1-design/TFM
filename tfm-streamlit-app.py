"""
TFM — MÓDULO SCREENER (Streamlit + Plotly)
Screening multi-índice con señales compuestas y score 0–10

Ejecutar:  streamlit run tfm_screener_app.py
Requisitos: pip install streamlit yfinance pandas numpy plotly requests
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from collections import Counter
import requests
import warnings
warnings.filterwarnings("ignore")


# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="TFM Screener — Investment Intelligence",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ═════════════════════════════════════════════════════════════════
# 1. FUNCIONES DE OBTENCIÓN DE TICKERS POR ÍNDICE
# ═════════════════════════════════════════════════════════════════
@st.cache_data(ttl=86400)
def obtener_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        tables = pd.read_html(requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).text)
        return [t.replace(".", "-") for t in tables[0]["Symbol"].tolist()]
    except Exception as e:
        st.warning(f"Error S&P500: {e}"); return []

@st.cache_data(ttl=86400)
def obtener_sp600_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
    try:
        tables = pd.read_html(requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).text)
        df = tables[0] if "Symbol" in tables[0].columns else tables[1]
        col = "Symbol" if "Symbol" in df.columns else "Ticker symbol"
        return [t.replace(".", "-") for t in df[col].tolist()]
    except Exception as e:
        st.warning(f"Error S&P600: {e}"); return []

@st.cache_data(ttl=86400)
def obtener_ibex35_tickers():
    tickers = [
        "SAN.MC","BBVA.MC","ITX.MC","IBE.MC","TEF.MC","FER.MC","AMS.MC",
        "REP.MC","CABK.MC","ACS.MC","GRF.MC","MAP.MC","ENG.MC","RED.MC",
        "IAG.MC","FDR.MC","MEL.MC","COL.MC","CLNX.MC","SAB.MC","BKT.MC",
        "AENA.MC","LOG.MC","CIE.MC","ACX.MC","MRL.MC","PHM.MC","ROVI.MC",
        "VIS.MC","ALM.MC","SGRE.MC","SLR.MC","UNI.MC","EDR.MC","SOL.MC",
    ]
    return tickers

@st.cache_data(ttl=86400)
def obtener_dax40_tickers():
    tickers = [
        "SAP.DE","SIE.DE","ALV.DE","DTE.DE","AIR.DE","MBG.DE","MUV2.DE",
        "DHL.DE","BAS.DE","BMW.DE","IFX.DE","BEI.DE","BAYN.DE","HEN3.DE",
        "ADS.DE","VOW3.DE","DB1.DE","SY1.DE","RWE.DE","FRE.DE","CON.DE",
        "MTX.DE","ENR.DE","HNR1.DE","PAH3.DE","ZAL.DE","HEI.DE","QIA.DE",
        "MRK.DE","SHL.DE","DTG.DE","DBK.DE","P911.DE","1COV.DE","BNR.DE",
        "LHA.DE","PUM.DE","TKA.DE","LEG.DE","VNA.DE",
    ]
    return tickers

@st.cache_data(ttl=86400)
def obtener_ftse100_tickers():
    tickers = [
        "AZN.L","SHEL.L","HSBA.L","ULVR.L","BP.L","GSK.L","RIO.L",
        "REL.L","DGE.L","LSEG.L","NG.L","BA.L","GLEN.L","VOD.L",
        "PRU.L","CPG.L","AAL.L","LLOY.L","BARC.L","BATS.L","AHT.L",
        "BKG.L","CRH.L","EXPN.L","IMB.L","NWG.L","RKT.L","SSE.L",
        "TSCO.L","WPP.L",
    ]
    return tickers


INDICES_DISPONIBLES = {
    "SP500":  ("S&P 500",     obtener_sp500_tickers),
    "SP600":  ("S&P 600 SmallCap", obtener_sp600_tickers),
    "IBEX35": ("IBEX 35",     obtener_ibex35_tickers),
    "DAX40":  ("DAX 40",      obtener_dax40_tickers),
    "FTSE100":("FTSE 100",    obtener_ftse100_tickers),
}


# ═════════════════════════════════════════════════════════════════
# 2. UTILIDADES
# ═════════════════════════════════════════════════════════════════
def _safe_get(series, idx=0, default=np.nan):
    try:
        v = series.iloc[idx]
        return v if not pd.isna(v) else default
    except:
        return default


def calcular_candle(row):
    """Clasifica la vela del día."""
    try:
        op, hi, lo, cl = row["Open"], row["High"], row["Low"], row["Close"]
        rango = hi - lo
        if rango == 0:
            return "Doji"
        cuerpo   = abs(cl - op)
        somb_inf = min(op, cl) - lo
        somb_sup = hi - max(op, cl)
        pct_c    = cuerpo / rango
        if pct_c < 0.15:                                       return "Doji"
        if somb_inf > cuerpo * 2 and somb_sup < cuerpo * 0.5:  return "Hammer"
        if somb_sup > cuerpo * 2 and somb_inf < cuerpo * 0.5:  return "ShootingStar"
        if cl > op and pct_c > 0.6:                             return "Marubozu+"
        if cl < op and pct_c > 0.6:                             return "Marubozu-"
        return "Alcista" if cl > op else "Bajista"
    except:
        return "N/A"


def calcular_chaikin_ad(hist):
    """Índice Chaikin Accumulation/Distribution normalizado."""
    try:
        rng = hist["High"] - hist["Low"]
        rng = rng.replace(0, np.nan)
        mfm = ((hist["Close"] - hist["Low"]) - (hist["High"] - hist["Close"])) / rng
        mfv = mfm * hist["Volume"]
        ad  = mfv.cumsum()
        ad_sma = ad.rolling(20).mean()
        return ad, ad_sma
    except:
        return pd.Series(dtype=float), pd.Series(dtype=float)


# ═════════════════════════════════════════════════════════════════
# 3. SCORE COMPUESTO 0–10
#    Técnico      (0–3.5) : momentum, medias, volumen
#    Fundamental  (0–4.0) : PER, ROE, márgenes, deuda
#    Price Action (0–2.5) : breakout, señales, potencial analistas
# ═════════════════════════════════════════════════════════════════
def _score_compuesto_screener(row):
    s = 0.0

    # ── TÉCNICO (max ~3.5) ────────────────────────────────────────
    mom3 = row.get("Mom 3M %", np.nan)
    if   pd.notna(mom3) and mom3 > 20: s += 1.0
    elif pd.notna(mom3) and mom3 > 10: s += 0.7
    elif pd.notna(mom3) and mom3 > 0:  s += 0.4

    sma50 = row.get("vs SMA50 %", np.nan)
    if pd.notna(sma50) and sma50 > 0: s += 0.5

    sma200 = row.get("vs SMA200 %", np.nan)
    if pd.notna(sma200) and sma200 > 0: s += 0.5

    vol_r = row.get("Vol/Avg 20d", np.nan)
    if   pd.notna(vol_r) and vol_r > 2.0: s += 1.0
    elif pd.notna(vol_r) and vol_r > 1.3: s += 0.5

    vol_t = row.get("Vol Tend 20/50", np.nan)
    if pd.notna(vol_t) and vol_t > 1.0: s += 0.5

    # ── FUNDAMENTAL (max ~4.0) ────────────────────────────────────
    per = row.get("PER", np.nan)
    if   pd.notna(per) and 0 < per < 12: s += 1.5
    elif pd.notna(per) and 0 < per < 20: s += 1.0
    elif pd.notna(per) and 0 < per < 30: s += 0.3

    roe = row.get("ROE %", np.nan)
    if   pd.notna(roe) and roe > 25: s += 1.0
    elif pd.notna(roe) and roe > 15: s += 0.7
    elif pd.notna(roe) and roe > 8:  s += 0.3

    mg = row.get("Margen Net %", np.nan)
    if   pd.notna(mg) and mg > 20: s += 1.0
    elif pd.notna(mg) and mg > 10: s += 0.7
    elif pd.notna(mg) and mg > 3:  s += 0.3

    de = row.get("D/E", np.nan)
    if   pd.notna(de) and de < 50:  s += 0.5
    elif pd.notna(de) and de < 100: s += 0.3

    # ── PRICE ACTION (max ~2.5) ───────────────────────────────────
    if row.get("Breakout 20d") == "SI":  s += 0.75
    if row.get("Near 52W High") == "SI": s += 0.5

    señales = str(row.get("Señales", ""))
    if "BREAKOUT"     in señales: s += 0.75
    if "VOL_CONFIRM"  in señales: s += 0.5
    if "ACUMULACION"  in señales: s += 0.5
    if "TRAMPA_ALC"   in señales: s -= 0.5
    if "CAPITULACION" in señales: s -= 0.3

    pot = row.get("Potencial %", np.nan)
    if   pd.notna(pot) and pot > 20: s += 0.5
    elif pd.notna(pot) and pot > 10: s += 0.3

    return round(min(max(s, 0.0), 10.0), 1)


def _label_score(score):
    if   score >= 7.5: return "COMPRAR"
    elif score >= 6.0: return "VIGILAR"
    elif score >= 4.0: return "NEUTRO"
    else:              return "EVITAR"


# ═════════════════════════════════════════════════════════════════
# 4. MOTOR DEL SCREENER — Análisis por ticker
# ═════════════════════════════════════════════════════════════════
def analizar_ticker_screener(ticker):
    """
    Analiza un ticker y devuelve dict con todas las métricas.
    Volumen · Acción de precio · Señales compuestas · Score 0–10 · Fundamental
    """
    try:
        stock = yf.Ticker(ticker)
        hist  = stock.history(period="1y")
        info  = stock.info

        if hist.empty or len(hist) < 60 or len(info) < 5:
            return None

        last  = hist.iloc[-1]
        prev  = hist.iloc[-2] if len(hist) > 1 else last
        precio = last["Close"]
        if precio <= 0:
            return None

        # ── Market Cap ──
        mktcap = info.get("marketCap", 0)
        mktcap_b = round(mktcap / 1e9, 2) if mktcap else 0

        # ── Momentum ──
        mom_1m = ((precio / hist["Close"].iloc[-21] - 1) * 100) if len(hist) > 21 else np.nan
        mom_3m = ((precio / hist["Close"].iloc[-63] - 1) * 100) if len(hist) > 63 else np.nan

        # ── Medias móviles ──
        sma50  = hist["Close"].rolling(50).mean().iloc[-1]  if len(hist) >= 50  else np.nan
        sma200 = hist["Close"].rolling(200).mean().iloc[-1] if len(hist) >= 200 else np.nan
        vs_sma50  = round((precio / sma50  - 1) * 100, 2) if pd.notna(sma50)  else np.nan
        vs_sma200 = round((precio / sma200 - 1) * 100, 2) if pd.notna(sma200) else np.nan

        # ── Distancia a extremos 52W ──
        max52 = hist["High"].max()
        min52 = hist["Low"].min()
        dist_max = round((precio / max52 - 1) * 100, 2) if max52 > 0 else np.nan
        dist_min = round((precio / min52 - 1) * 100, 2) if min52 > 0 else np.nan

        # ── Breakout ──
        max_20d = hist["High"].iloc[-21:-1].max() if len(hist) > 21 else np.nan
        breakout_20d = "SI" if pd.notna(max_20d) and precio > max_20d else "NO"
        near_52w_hi  = "SI" if pd.notna(dist_max) and abs(dist_max) < 3 else "NO"

        # ── Gap y rango ──
        gap_pct    = round((last["Open"] / prev["Close"] - 1) * 100, 2) if prev["Close"] > 0 else 0
        rango_pct  = round((last["High"] - last["Low"]) / precio * 100, 2) if precio > 0 else 0

        # ── Días consecutivos ──
        cambios = hist["Close"].diff()
        dias_c  = 0
        for v in cambios.iloc[::-1]:
            if pd.isna(v): break
            if v > 0 and dias_c >= 0:
                dias_c = dias_c + 1 if dias_c > 0 else 1
            elif v < 0 and dias_c <= 0:
                dias_c = dias_c - 1 if dias_c < 0 else -1
            else:
                break

        # ── Vela ──
        vela = calcular_candle(last)

        # ── Volumen ──
        vol_hoy    = last["Volume"]
        vol_avg20  = hist["Volume"].rolling(20).mean().iloc[-1]
        vol_avg50  = hist["Volume"].rolling(50).mean().iloc[-1]
        vol_ratio_20d = round(vol_hoy / vol_avg20, 2) if vol_avg20 > 0 else np.nan
        vol_ratio_50d = round(vol_avg20 / vol_avg50, 2) if vol_avg50 > 0 else np.nan

        vol_5d  = hist["Volume"].iloc[-5:].mean()
        vol_20d = hist["Volume"].iloc[-20:].mean()
        if vol_20d > 0:
            ratio_5_20 = vol_5d / vol_20d
            if   ratio_5_20 > 1.15: vol_tendencia = "Creciente"
            elif ratio_5_20 < 0.85: vol_tendencia = "Decreciente"
            else:                    vol_tendencia = "Estable"
        else:
            vol_tendencia = "N/A"

        # ── Chaikin A/D ──
        ad, ad_sma = calcular_chaikin_ad(hist)
        if not ad.empty and not ad_sma.empty:
            ad_last = ad.iloc[-1]
            ad_sma_last = ad_sma.iloc[-1]
            if pd.notna(ad_last) and pd.notna(ad_sma_last):
                acum_label = "Acumulación" if ad_last > ad_sma_last else "Distribución"
            else:
                acum_label = "N/A"
        else:
            acum_label = "N/A"

        # ── SEÑALES COMPUESTAS ──
        señales = []
        ret_dia = (precio / prev["Close"] - 1) if prev["Close"] > 0 else 0

        # VOL_CONFIRM: precio sube + vol > 1.5x
        if ret_dia > 0 and pd.notna(vol_ratio_20d) and vol_ratio_20d > 1.5:
            señales.append("VOL_CONFIRM")

        # TRAMPA_ALC: precio sube pero vol < 0.7x
        if ret_dia > 0 and pd.notna(vol_ratio_20d) and vol_ratio_20d < 0.7:
            señales.append("TRAMPA_ALC")

        # BREAKOUT: rompe max 20d + vol > 1.5x
        if breakout_20d == "SI" and pd.notna(vol_ratio_20d) and vol_ratio_20d > 1.5:
            señales.append("BREAKOUT")

        # NEAR_52W_HI
        if near_52w_hi == "SI":
            señales.append("NEAR_52W_HI")

        # ACUMULACION: zona baja (dist_min < 30%) + vol creciente
        if pd.notna(dist_min) and dist_min < 30 and vol_tendencia == "Creciente" and acum_label == "Acumulación":
            señales.append("ACUMULACION")

        # CAPITULACION: caída >3% + vol > 2.5x
        if ret_dia < -0.03 and pd.notna(vol_ratio_20d) and vol_ratio_20d > 2.5:
            señales.append("CAPITULACION")

        # HAMMER_GIRO: vela hammer en sobreventa
        rsi_14 = None
        try:
            delta = hist["Close"].diff()
            gain  = delta.where(delta > 0, 0).rolling(14).mean()
            loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs    = gain / loss
            rsi_s = 100 - (100 / (1 + rs))
            rsi_14 = rsi_s.iloc[-1]
        except:
            pass
        if vela == "Hammer" and rsi_14 is not None and rsi_14 < 35:
            señales.append("HAMMER_GIRO")

        # MOMENTUM: retorno 3M > 15% + volumen creciente
        if pd.notna(mom_3m) and mom_3m > 15 and vol_tendencia == "Creciente":
            señales.append("MOMENTUM")

        señales_str = " | ".join(señales) if señales else "-"

        # ── Fundamental ──
        per       = info.get("trailingPE")
        per_fw    = info.get("forwardPE")
        peg       = info.get("pegRatio")
        roe       = info.get("returnOnEquity")
        margen_n  = info.get("profitMargins")
        margen_b  = info.get("grossMargins")
        de        = info.get("debtToEquity")
        dy        = info.get("dividendYield")
        rec       = info.get("recommendationKey", "N/A")
        target_m  = info.get("targetMeanPrice")
        potencial = round((target_m / precio - 1) * 100, 1) if target_m and precio > 0 else np.nan

        # FCF Yield
        fcf_yield = np.nan
        try:
            fcf    = info.get("freeCashflow")
            mktcap_v = info.get("marketCap")
            if fcf and mktcap_v and mktcap_v > 0:
                fcf_yield = round(fcf / mktcap_v * 100, 2)
        except:
            pass

        resultado = {
            "Ticker":         ticker,
            "Precio":         round(precio, 2),
            "MktCap (B$)":    mktcap_b,
            "Mom 1M %":       round(mom_1m, 2) if pd.notna(mom_1m) else np.nan,
            "Mom 3M %":       round(mom_3m, 2) if pd.notna(mom_3m) else np.nan,
            "vs SMA50 %":     vs_sma50,
            "vs SMA200 %":    vs_sma200,
            "Dist Max52W %":  dist_max,
            "Dist Min52W %":  dist_min,
            "Breakout 20d":   breakout_20d,
            "Near 52W High":  near_52w_hi,
            "Gap %":          gap_pct,
            "Rango Intra %":  rango_pct,
            "Días Consec":    dias_c,
            "Vela":           vela,
            "Vol/Avg 20d":    vol_ratio_20d,
            "Vol Tend 20/50": vol_ratio_50d,
            "Vol Tendencia":  vol_tendencia,
            "Acum/Distrib":   acum_label,
            "Señales":        señales_str,
            "PER":            round(per,    1) if per    else np.nan,
            "PER Fwd":        round(per_fw, 1) if per_fw else np.nan,
            "PEG":            round(peg,    2) if peg    else np.nan,
            "ROE %":          round(roe * 100, 1) if roe else np.nan,
            "Margen Net %":   round(margen_n * 100, 1) if margen_n else np.nan,
            "Margen Bruto %": round(margen_b * 100, 1) if margen_b else np.nan,
            "D/E":            round(de, 1) if de else np.nan,
            "FCF Yield %":    fcf_yield,
            "Div Yield %":    round(dy * 100, 2) if dy else 0,
            "Potencial %":    potencial,
            "Consenso":       rec,
        }

        resultado["Score"] = _score_compuesto_screener(resultado)
        resultado["Label"] = _label_score(resultado["Score"])

        return resultado

    except:
        return None


# ═════════════════════════════════════════════════════════════════
# 5. FILTROS POR MODO
# ═════════════════════════════════════════════════════════════════
def aplicar_filtros(df, modo):
    d = df.copy()
    if modo == "VALUE":
        mask  = d["PER"].between(0, 20, inclusive="right") & (d["Margen Net %"] > 8) & (d["D/E"] < 150)
        label = "VALUE — Baratas con buenos márgenes"
    elif modo == "MOMENTUM":
        mask  = (d["Mom 3M %"] > 10) & (d["Vol Tend 20/50"] > 1.0) & (d["vs SMA50 %"] > 0)
        label = "MOMENTUM — Tendencia fuerte con volumen"
    elif modo == "BREAKOUT":
        mask  = (d["Breakout 20d"] == "SI") & (d["Vol/Avg 20d"] > 1.3)
        label = "BREAKOUT — Precio rompiendo resistencias con volumen"
    elif modo == "ACUMULACION":
        mask  = (
            (d["Dist Min52W %"] < 30)
            & d["Acum/Distrib"].str.contains("Acum", na=False)
            & d["Vol Tendencia"].str.contains("Crec", na=False)
        )
        label = "ACUMULACIÓN — Zona baja con presión compradora"
    elif modo == "DIVIDENDOS":
        mask  = (d["Div Yield %"] > 2.5) & (d["Margen Net %"] > 5) & (d["D/E"] < 200)
        label = "DIVIDENDOS — Rentabilidad estable"
    elif modo == "QUALITY":
        mask  = (d["ROE %"] > 15) & (d["Margen Net %"] > 12) & (d["Mom 3M %"] > 0)
        label = "QUALITY — Alta rentabilidad y crecimiento"
    else:
        mask  = pd.Series([True] * len(d), index=d.index)
        label = "TODOS LOS RESULTADOS"
    return d[mask].copy(), label


# ═════════════════════════════════════════════════════════════════
# 6. VISUALIZACIÓN — 4 PANELES (Plotly)
# ═════════════════════════════════════════════════════════════════
COLORES_LABEL = {
    "COMPRAR": "#50fa7b",
    "VIGILAR": "#8be9fd",
    "NEUTRO":  "#f1fa8c",
    "EVITAR":  "#ff5555",
}

COLORES_SEÑAL = {
    "BREAKOUT":     "#50fa7b",
    "VOL_CONFIRM":  "#8be9fd",
    "ACUMULACION":  "#bd93f9",
    "MOMENTUM":     "#ffb86c",
    "NEAR_52W_HI":  "#f1fa8c",
    "HAMMER_GIRO":  "#ff79c6",
    "CAPITULACION": "#ff5555",
    "TRAMPA_ALC":   "#6272a4",
}


def visualizar_screener_plotly(df_resultado, titulo="Screener — Mapa de Señales", top_n=25):
    """Genera 4 gráficos Plotly interactivos."""
    if df_resultado is None or df_resultado.empty:
        st.warning("Sin datos para visualizar.")
        return

    df = df_resultado.copy()
    for c in ["Mom 3M %", "Vol/Avg 20d", "MktCap (B$)", "Dist Max52W %"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Mom 3M %", "Vol/Avg 20d"]).head(top_n)
    if df.empty:
        st.warning("Sin datos numéricos para el gráfico.")
        return

    # ── Panel 1: Scatter Momentum vs Volumen ──
    fig1 = px.scatter(
        df, x="Mom 3M %", y="Vol/Avg 20d",
        color="Label",
        color_discrete_map=COLORES_LABEL,
        size="MktCap (B$)",
        hover_data=["Ticker", "Score", "Señales", "PER", "ROE %"],
        text="Ticker",
        title="Momentum 3M vs Volumen Relativo",
    )
    fig1.update_traces(textposition="top center", textfont_size=9)
    fig1.add_hline(y=1.5, line_dash="dash", line_color="gray", opacity=0.5,
                   annotation_text="Vol 1.5x")
    fig1.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.5)
    fig1.update_layout(
        template="plotly_dark", height=500,
        paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e",
    )
    st.plotly_chart(fig1, use_container_width=True)

    col1, col2 = st.columns(2)

    # ── Panel 2: Score por ticker (barras horizontales) ──
    with col1:
        df_sorted = df.sort_values("Score", ascending=True).tail(20)
        colors_bar = [COLORES_LABEL.get(l, "#6272a4") for l in df_sorted["Label"]]
        fig2 = go.Figure(go.Bar(
            y=df_sorted["Ticker"], x=df_sorted["Score"],
            orientation="h",
            marker_color=colors_bar,
            text=[f"{s:.1f}" for s in df_sorted["Score"]],
            textposition="outside",
            hovertext=[
                f"{t}<br>Score: {s:.1f}<br>Label: {l}<br>{sig}"
                for t, s, l, sig in zip(
                    df_sorted["Ticker"], df_sorted["Score"],
                    df_sorted["Label"], df_sorted["Señales"]
                )
            ],
        ))
        fig2.add_vline(x=7.5, line_dash="dash", line_color="#50fa7b", opacity=0.5,
                       annotation_text="COMPRAR")
        fig2.add_vline(x=6.0, line_dash="dash", line_color="#8be9fd", opacity=0.3)
        fig2.add_vline(x=4.0, line_dash="dash", line_color="#f1fa8c", opacity=0.3)
        fig2.update_layout(
            title="Score Compuesto 0–10",
            xaxis_title="Score", xaxis_range=[0, 10.5],
            template="plotly_dark", height=500,
            paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e",
        )
        st.plotly_chart(fig2, use_container_width=True)

    # ── Panel 3: Distribución de señales ──
    with col2:
        todas = " | ".join(df["Señales"].fillna("-").tolist())
        tipos = [s.strip() for s in todas.split("|") if s.strip() not in ("-", "")]
        if tipos:
            counter = Counter(tipos)
            señales_df = pd.DataFrame(
                counter.most_common(10), columns=["Señal", "Conteo"]
            )
            colors_sig = [COLORES_SEÑAL.get(s, "#6272a4") for s in señales_df["Señal"]]
            fig3 = go.Figure(go.Bar(
                x=señales_df["Señal"], y=señales_df["Conteo"],
                marker_color=colors_sig,
                text=señales_df["Conteo"],
                textposition="outside",
            ))
            fig3.update_layout(
                title="Distribución de Señales Compuestas",
                yaxis_title="Nº de acciones",
                template="plotly_dark", height=500,
                paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e",
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No se detectaron señales compuestas en este grupo.")

    # ── Panel 4: Distancia al máximo 52W vs Momentum ──
    if "Dist Max52W %" in df.columns:
        fig4 = px.scatter(
            df, x="Dist Max52W %", y="Mom 3M %",
            color="Label",
            color_discrete_map=COLORES_LABEL,
            hover_data=["Ticker", "Score", "Vol/Avg 20d"],
            text="Ticker",
            title="Distancia al Máx 52W vs Momentum 3M",
        )
        fig4.update_traces(textposition="top center", textfont_size=9)
        fig4.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.4)
        fig4.add_vline(x=-3, line_dash="dash", line_color="#50fa7b", opacity=0.4,
                       annotation_text="Near High")
        fig4.update_layout(
            template="plotly_dark", height=450,
            paper_bgcolor="#12121f", plot_bgcolor="#1e1e2e",
        )
        st.plotly_chart(fig4, use_container_width=True)


# ═════════════════════════════════════════════════════════════════
# 7. INTERFAZ STREAMLIT
# ═════════════════════════════════════════════════════════════════

# ── Sidebar ──
with st.sidebar:
    st.title("🔍 TFM Screener")
    st.caption("Master IA Sector Financiero — VIU")
    st.divider()

    indice = st.selectbox(
        "Índice",
        list(INDICES_DISPONIBLES.keys()),
        format_func=lambda x: INDICES_DISPONIBLES[x][0],
    )
    modo = st.selectbox(
        "Modo de screening",
        ["VALUE", "MOMENTUM", "BREAKOUT", "ACUMULACION", "DIVIDENDOS", "QUALITY", "TODO"],
    )
    limite = st.slider("Tickers a analizar", 10, 200, 80, step=10)
    min_mktcap = st.number_input("Market Cap mínimo ($B)", value=0.5, step=0.5, min_value=0.0)

    st.divider()
    ejecutar = st.button("🚀 Ejecutar Screener", type="primary", use_container_width=True)


# ── Contenido principal ──
st.header(f"🔍 Screener: {INDICES_DISPONIBLES[indice][0]} — {modo}")

# Descripción del modo
MODO_DESC = {
    "VALUE":       "PER 0–20, margen neto > 8%, D/E < 150",
    "MOMENTUM":    "Momentum 3M > 10%, vol. tendencia > 1x, sobre SMA50",
    "BREAKOUT":    "Ruptura máx 20d + volumen > 1.3x media",
    "ACUMULACION": "Zona baja + Chaikin A/D en acumulación + vol. creciente",
    "DIVIDENDOS":  "Yield > 2.5%, margen neto > 5%, D/E < 200",
    "QUALITY":     "ROE > 15%, margen neto > 12%, momentum positivo",
    "TODO":        "Sin filtro adicional — todos los resultados",
}
st.caption(f"Filtro: {MODO_DESC.get(modo, '')}")


if ejecutar:
    nombre_indice = INDICES_DISPONIBLES[indice][0]

    # ── Obtener tickers ──
    with st.spinner(f"Obteniendo tickers de {nombre_indice}..."):
        fn_tickers = INDICES_DISPONIBLES[indice][1]
        tickers = fn_tickers()

    if not tickers:
        st.error("No se pudieron obtener los tickers.")
    else:
        tickers_a_analizar = tickers[:limite]
        st.info(f"{len(tickers)} tickers cargados. Analizando los primeros {len(tickers_a_analizar)}...")

        # ── Analizar uno a uno con barra de progreso ──
        resultados = []
        progress_bar = st.progress(0, text="Analizando...")
        status_text = st.empty()

        for i, ticker in enumerate(tickers_a_analizar):
            progress_bar.progress(
                (i + 1) / len(tickers_a_analizar),
                text=f"Analizando {ticker} ({i+1}/{len(tickers_a_analizar)})"
            )
            resultado = analizar_ticker_screener(ticker)
            if resultado:
                # Filtro de market cap
                if resultado.get("MktCap (B$)", 0) >= min_mktcap:
                    resultados.append(resultado)

        progress_bar.empty()
        status_text.empty()

        if not resultados:
            st.error("Ningún ticker devolvió resultados válidos.")
        else:
            df_raw = pd.DataFrame(resultados)

            # ── Aplicar filtros del modo ──
            df_filtered, label_modo = aplicar_filtros(df_raw, modo)
            df_filtered = df_filtered.sort_values("Score", ascending=False).reset_index(drop=True)

            # ── Métricas resumen ──
            st.subheader(f"Resultados: {len(df_filtered)} de {len(df_raw)} — {label_modo}")

            if df_filtered.empty:
                st.warning("Ningún activo cumple los filtros de este modo. Prueba con 'TODO'.")
            else:
                # KPIs rápidos
                kc1, kc2, kc3, kc4, kc5 = st.columns(5)
                kc1.metric("Acciones filtradas", len(df_filtered))
                kc2.metric("Score medio", f"{df_filtered['Score'].mean():.1f}")
                n_comprar = len(df_filtered[df_filtered["Label"] == "COMPRAR"])
                kc3.metric("🟢 COMPRAR", n_comprar)
                n_vigilar = len(df_filtered[df_filtered["Label"] == "VIGILAR"])
                kc4.metric("🔵 VIGILAR", n_vigilar)
                mom_med = df_filtered["Mom 3M %"].median()
                kc5.metric("Momentum 3M med.", f"{mom_med:+.1f}%" if pd.notna(mom_med) else "N/A")

                # ── Tabla principal ──
                st.subheader("Tabla de Resultados")

                # Columnas a mostrar (ordenadas por relevancia)
                cols_mostrar = [
                    "Ticker", "Precio", "Score", "Label", "Señales",
                    "Mom 3M %", "Vol/Avg 20d", "Vol Tendencia", "Acum/Distrib",
                    "vs SMA50 %", "Dist Max52W %", "Breakout 20d",
                    "PER", "ROE %", "Margen Net %", "D/E",
                    "Div Yield %", "Potencial %", "Consenso", "MktCap (B$)",
                ]
                cols_disponibles = [c for c in cols_mostrar if c in df_filtered.columns]

                # Colorear labels
                def color_label(val):
                    c = COLORES_LABEL.get(val, "")
                    return f"color: {c}; font-weight: bold" if c else ""

                def color_score(val):
                    try:
                        v = float(val)
                        if v >= 7.5: return "background-color: rgba(80,250,123,0.3)"
                        if v >= 6.0: return "background-color: rgba(139,233,253,0.2)"
                        if v >= 4.0: return "background-color: rgba(241,250,140,0.15)"
                        return "background-color: rgba(255,85,85,0.2)"
                    except:
                        return ""

                def color_señales(val):
                    s = str(val)
                    if "BREAKOUT" in s:    return "color: #50fa7b"
                    if "VOL_CONFIRM" in s: return "color: #8be9fd"
                    if "ACUMULACION" in s: return "color: #bd93f9"
                    if "TRAMPA_ALC" in s:  return "color: #ff5555"
                    return ""

                styled = (
                    df_filtered[cols_disponibles]
                    .style
                    .map(color_label, subset=["Label"])
                    .map(color_score, subset=["Score"])
                    .map(color_señales, subset=["Señales"])
                    .format(na_rep="-", precision=2)
                )
                st.dataframe(styled, use_container_width=True, height=500)

                # ── Gráficos interactivos ──
                st.subheader("📊 Visualización de Señales")
                visualizar_screener_plotly(
                    df_filtered,
                    titulo=f"{nombre_indice} — {modo}",
                    top_n=25,
                )

                # ── Guardar en session_state para otros módulos ──
                st.session_state["screener_results"]  = df_filtered
                st.session_state["screener_raw"]       = df_raw
                st.session_state["screener_indice"]    = nombre_indice
                st.session_state["screener_modo"]      = modo

                # ── Exportar CSV ──
                st.divider()
                csv = df_filtered.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "📥 Descargar resultados (CSV)",
                    csv,
                    f"screener_{indice}_{modo}.csv",
                    "text/csv",
                )

# ── Si hay resultados previos, mostrarlos ──
elif "screener_results" in st.session_state:
    st.info(
        f"Mostrando últimos resultados: "
        f"{st.session_state['screener_indice']} — {st.session_state['screener_modo']}"
    )
    df_prev = st.session_state["screener_results"]

    cols_mostrar = [
        "Ticker", "Precio", "Score", "Label", "Señales",
        "Mom 3M %", "Vol/Avg 20d", "Vol Tendencia",
        "PER", "ROE %", "Margen Net %", "Potencial %",
    ]
    cols_disponibles = [c for c in cols_mostrar if c in df_prev.columns]
    st.dataframe(df_prev[cols_disponibles], use_container_width=True, height=400)

    visualizar_screener_plotly(df_prev, top_n=25)

else:
    st.info("👈 Configura los parámetros en la barra lateral y pulsa **Ejecutar Screener**.")
    st.markdown(
        """
        **Modos disponibles:**

        | Modo | Descripción |
        |------|-------------|
        | VALUE | PER bajo, buenos márgenes, deuda controlada |
        | MOMENTUM | Tendencia fuerte con volumen creciente |
        | BREAKOUT | Rupturas de precio con confirmación de volumen |
        | ACUMULACIÓN | Zona baja con presión compradora institucional |
        | DIVIDENDOS | Yield atractivo con fundamentales sólidos |
        | QUALITY | Alta rentabilidad y crecimiento sostenido |
        | TODO | Sin filtro — análisis completo del universo |
        """
    )

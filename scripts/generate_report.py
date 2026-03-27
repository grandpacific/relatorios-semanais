"""
GrandPacific — Gerador de Relatório Semanal v2
Foco: Açúcar & Soja — preços, exportação, compradores globais, posições
Fluxo: Coleta dados → Claude API → Sanity CMS
"""

import os
import re
import json
import unicodedata
import requests
from datetime import datetime, timedelta
from typing import Optional
import anthropic
import yfinance as yf

# ── CONFIGURAÇÃO ─────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
SANITY_PROJECT_ID = os.getenv("SANITY_PROJECT_ID", "")
SANITY_DATASET    = os.getenv("SANITY_DATASET", "production")
SANITY_API_TOKEN  = os.getenv("SANITY_API_TOKEN", "")
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")
OPEN_WEATHER_KEY  = os.getenv("OPEN_WEATHER_KEY", "")


# ── 1. PREÇOS VIA YAHOO FINANCE ───────────────────────────────────────────────

def get_futures_price(ticker_symbol: str, name: str, unit: str) -> dict:
    """Busca preço atual, variação semanal e mensal de um futuro."""
    try:
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="25d", interval="1d")
        if df.empty:
            return {}
        price_now  = round(float(df["Close"].iloc[-1]), 2)
        price_5d   = round(float(df["Close"].iloc[-6])  if len(df) >= 6  else float(df["Close"].iloc[0]), 2)
        price_20d  = round(float(df["Close"].iloc[-21]) if len(df) >= 21 else float(df["Close"].iloc[0]), 2)
        var_5d     = round(((price_now - price_5d)  / price_5d)  * 100, 2) if price_5d  else 0
        var_20d    = round(((price_now - price_20d) / price_20d) * 100, 2) if price_20d else 0
        result = {
            "name":      name,
            "ticker":    ticker_symbol,
            "price":     price_now,
            "price_5d":  price_5d,
            "price_20d": price_20d,
            "var_5d":    var_5d,
            "var_20d":   var_20d,
            "unit":      unit,
            "date":      df.index[-1].strftime("%Y-%m-%d"),
            "source":    "Yahoo Finance",
        }
        direction = "▲" if var_5d > 0 else "▼"
        print(f"   ✓ {name}: {price_now} {unit} ({direction}{abs(var_5d):.1f}% 5d)")
        return result
    except Exception as e:
        print(f"   [WARN] {name} ({ticker_symbol}): {e}")
        return {}


def get_all_prices() -> dict:
    """Busca preços de todos os instrumentos relevantes para GPG."""
    print("→ Buscando preços (Yahoo Finance)...")
    return {
        # ── FOCO PRINCIPAL ──────────────────────────────────────────
        "acucar_11":  get_futures_price("SB=F",     "Sugar #11 (raw/bruto)",  "USd/lb"),
        "acucar_5":   get_futures_price("SF=F",     "Sugar #5 (white/branco)","USD/ton"),
        "soja":       get_futures_price("ZS=F",     "Soybeans",               "USd/bu"),
        "soja_meal":  get_futures_price("ZM=F",     "Soybean Meal",           "USD/ton"),
        "soja_oil":   get_futures_price("ZL=F",     "Soybean Oil",            "USd/lb"),
        # ── CONTEXTO ────────────────────────────────────────────────
        "milho":      get_futures_price("ZC=F",     "Corn",                   "USd/bu"),
        "trigo":      get_futures_price("ZW=F",     "Wheat",                  "USd/bu"),
        "cafe":       get_futures_price("KC=F",     "Coffee Arabica",         "USd/lb"),
        # ── CÂMBIO ──────────────────────────────────────────────────
        "usd_brl":    get_futures_price("USDBRL=X", "USD/BRL",                "BRL"),
        "usd_idx":    get_futures_price("DX-Y.NYB", "USD Index (DXY)",        "pts"),
    }


def calc_sugar_spread(prices: dict) -> dict:
    """
    Calcula spread Sugar #5 (branco) vs Sugar #11 (bruto) em USD/ton.
    Spread > $80/ton = janela favorável para ICUMSA 45.
    """
    try:
        raw   = prices.get("acucar_11", {}).get("price", 0)
        white = prices.get("acucar_5",  {}).get("price", 0)
        if not raw or not white:
            return {}
        raw_per_ton = round(raw * 22.0462, 2)
        spread      = round(white - raw_per_ton, 2)
        if spread > 100:
            interpretation = "Spread muito favorável — forte incentivo ao refinamento e exportação de ICUMSA 45"
        elif spread > 80:
            interpretation = "Spread favorável — margem de refinamento acima da média histórica"
        elif spread > 60:
            interpretation = "Spread neutro — dentro da faixa histórica normal"
        else:
            interpretation = "Spread comprimido — mercado prefere açúcar bruto, menor incentivo ao refinamento"
        return {
            "raw_usd_ton":    raw_per_ton,
            "white_usd_ton":  white,
            "spread_usd_ton": spread,
            "interpretation": interpretation,
            "signal":         "buy_white" if spread > 80 else "buy_raw" if spread < 60 else "neutral",
        }
    except:
        return {}


# ── 2. EXPORTAÇÕES BRASILEIRAS (MDIC/Comex Stat) ─────────────────────────────

def get_comex_export(ncm: str, name: str) -> dict:
    """Exportações via API pública MDIC/Comex Stat — gratuita, sem chave."""
    try:
        year  = datetime.now().year
        month = datetime.now().month
        url = (
            f"https://api-comexstat.mdic.gov.br/general?"
            f"flow=export&"
            f"monthYear={year-1}-01,{year}-{month:02d}&"
            f"ncm={ncm}&"
            f"groupBy=monthYear&"
            f"totals=true"
        )
        r = requests.get(url, timeout=12, headers={"Accept": "application/json"})
        if r.status_code != 200:
            return {"name": name, "note": "API indisponível"}
        data  = r.json()
        rows  = data.get("data", {}).get("list", [])
        if not rows:
            return {"name": name, "note": "Sem dados"}
        recent = sorted(rows, key=lambda x: x.get("monthYear", ""), reverse=True)[:3]
        print(f"   ✓ Exportação {name}: {len(recent)} meses")
        return {
            "name":    name,
            "ncm":     ncm,
            "months":  [
                {
                    "period":       r.get("monthYear", ""),
                    "fob_usd":      r.get("metricFOB", 0),
                    "kg_net":       r.get("metricKGLiquido", 0),
                }
                for r in recent
            ],
            "source": "MDIC/Comex Stat",
        }
    except Exception as e:
        print(f"   [WARN] Comex {name}: {e}")
        return {"name": name, "note": f"Erro: {str(e)[:50]}"}


def get_export_data() -> dict:
    """Coleta exportações de açúcar e soja."""
    print("→ Buscando exportações (MDIC/Comex Stat)...")
    return {
        "acucar_bruto":    get_comex_export("17011300", "Açúcar VHP/bruto"),
        "acucar_refinado": get_comex_export("17019900", "Açúcar ICUMSA 45/refinado"),
        "soja_grao":       get_comex_export("12019000", "Soja em grão"),
        "farelo_soja":     get_comex_export("23040000", "Farelo de soja"),
        "oleo_soja":       get_comex_export("15079000", "Óleo de soja bruto"),
    }


# ── 3. COMPRADORES GLOBAIS ────────────────────────────────────────────────────

def get_global_buyers(usd_brl: float) -> dict:
    """Perfil dos principais compradores globais de açúcar e soja."""
    print("→ Montando perfil de compradores globais...")
    return {
        "acucar": {
            "principais_compradores": [
                {
                    "pais": "China",
                    "share_pct": 18,
                    "perfil": "Maior importador global. Compras via COFCO e SINOGRAIN. Reservas estratégicas influenciam timing das compras.",
                    "status_atual": "Monitorar nível de estoques e câmbio CNY/USD"
                },
                {
                    "pais": "Índia",
                    "share_pct": 12,
                    "perfil": "Principal risco de oferta concorrente. Quando a safra doméstica (Maharashtra/UP) é boa, Índia exporta e pressiona preços. Deficit = demanda global.",
                    "status_atual": "Safra 2025/26 — acompanhar boletim ISMA"
                },
                {
                    "pais": "Oriente Médio (Arábia Saudita, EAU, Egito)",
                    "share_pct": 15,
                    "perfil": "Preferem ICUMSA 45. Compras frequentes e previsíveis. Boa base para contratos de médio prazo.",
                    "status_atual": "Demanda estável e recorrente"
                },
                {
                    "pais": "Indonésia",
                    "share_pct": 7,
                    "perfil": "Crescimento acelerado. Importações controladas pelo BULOG. Janelas de compra concentradas em poucos períodos.",
                    "status_atual": "Aguardar abertura de quota de importação"
                },
                {
                    "pais": "Bangladesh",
                    "share_pct": 8,
                    "perfil": "Comprador consistente de açúcar bruto para refinarias locais. Sensível ao câmbio BDT/USD.",
                    "status_atual": "Demanda regular, baixa volatilidade"
                },
            ],
            "posicao_brasil": "Maior exportador global — ~50% do comércio mundial",
            "impacto_cambio": f"USD/BRL a {usd_brl:.2f} — {'favorável ao exportador brasileiro' if usd_brl > 5.0 else 'margem pressionada'}",
        },
        "soja": {
            "principais_compradores": [
                {
                    "pais": "China",
                    "share_pct": 65,
                    "perfil": "Destino dominante. Compras via COFCO, SINOGRAIN e tradings privadas. Ciclo ligado à produção de ração suína e avícola.",
                    "status_atual": "Monitorar estoques COFCO e demanda por ração"
                },
                {
                    "pais": "União Europeia",
                    "share_pct": 10,
                    "perfil": "Demanda por farelo (proteína animal) e óleo. Regulação EUDR exige rastreabilidade de origem.",
                    "status_atual": "EUDR em vigor — rastreabilidade é requisito crescente"
                },
                {
                    "pais": "Tailândia / ASEAN",
                    "share_pct": 6,
                    "perfil": "Hub regional para ração animal e aquicultura. Crescimento acelerado.",
                    "status_atual": "Demanda em expansão"
                },
                {
                    "pais": "Bangladesh / Paquistão",
                    "share_pct": 4,
                    "perfil": "Farelo para aquicultura e aves. Crescimento de longo prazo.",
                    "status_atual": "Demanda regular"
                },
            ],
            "posicao_brasil": "Maior exportador global — ~50% das exportações mundiais",
            "impacto_cambio": f"USD/BRL a {usd_brl:.2f} — {'câmbio depreciado amplia competitividade frente à Argentina e EUA' if usd_brl > 5.0 else 'câmbio apreciado reduz vantagem competitiva'}",
        },
    }


# ── 4. CLIMA ───────────────────────────────────────────────────────────────────

def get_climate_data() -> list[str]:
    """Clima nas principais regiões produtoras de açúcar e soja."""
    regions = [
        {"name": "Sorriso (MT) — soja",        "lat": -12.5483, "lon": -55.7219},
        {"name": "Ribeirão Preto (SP) — cana",  "lat": -21.1699, "lon": -47.8096},
        {"name": "Araçatuba (SP) — cana",       "lat": -21.2088, "lon": -50.4329},
        {"name": "Londrina (PR) — soja",        "lat": -23.3045, "lon": -51.1696},
        {"name": "Rio Verde (GO) — soja",       "lat": -17.7983, "lon": -50.9283},
    ]
    alerts = []
    for region in regions:
        try:
            url = (
                f"https://api.openweathermap.org/data/2.5/weather"
                f"?lat={region['lat']}&lon={region['lon']}"
                f"&appid={OPEN_WEATHER_KEY}&units=metric&lang=pt_br"
            )
            r    = requests.get(url, timeout=8)
            data = r.json()
            desc = data.get("weather", [{}])[0].get("description", "")
            temp = data.get("main", {}).get("temp", 0)
            hum  = data.get("main", {}).get("humidity", 0)
            rain = data.get("rain", {}).get("1h", 0)
            s    = f"{region['name']}: {desc}, {temp:.0f}°C, umidade {hum}%"
            if rain:
                s += f", chuva {rain:.1f}mm/h"
            alerts.append(s)
        except Exception as e:
            print(f"   [WARN] Clima {region['name']}: {e}")
    return alerts


# ── 5. COLETA COMPLETA ────────────────────────────────────────────────────────

def collect_all_data() -> dict:
    """Agrega todos os dados para o relatório."""
    print("\n" + "="*60)
    print("  GrandPacific — Coleta de Dados v2 (Açúcar & Soja)")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("="*60 + "\n")

    week   = datetime.now().isocalendar()[1]
    year   = datetime.now().year
    prices = get_all_prices()
    spread = calc_sugar_spread(prices)
    usd_brl = prices.get("usd_brl", {}).get("price", 0.0)

    return {
        "semana":       week,
        "ano":          year,
        "data_geracao": datetime.now().isoformat(),
        "precos":       prices,
        "sugar_spread": spread,
        "exportacoes":  get_export_data(),
        "compradores":  get_global_buyers(usd_brl),
        "clima":        get_climate_data(),
    }


# ── 6. SYSTEM PROMPT ──────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Você é o analista-chefe da GrandPacific, empresa especializada em trading
de commodities agrícolas físicas — com foco em Açúcar (ICUMSA 45, VHP, bruto) e Soja
(grão, farelo, óleo) para exportação global a partir do Brasil.

MISSÃO: Produzir análise semanal que demonstra que a GrandPacific está no centro do mercado —
com dados de exportação, posições de compradores globais e inteligência de preço que o
cliente individual não consegue sozinho.

REGRAS ABSOLUTAS:
1. Use EXCLUSIVAMENTE os dados fornecidos. Nunca invente preços, volumes ou percentuais.
2. Se um dado não estiver disponível, diga "dado indisponível esta semana" e siga em frente.
3. Foco em Açúcar e Soja — mínimo 60% da análise nestes dois produtos.
4. Conecte sempre os dados de preço ao comportamento dos compradores globais.
5. O spread Sugar #5/#11 é inteligência proprietária — explore-o quando disponível.
6. Quantifique o impacto do câmbio para o exportador brasileiro.
7. A última seção SEMPRE mostra como a GrandPacific converte inteligência em vantagem concreta.
8. Tom: advisor experiente falando diretamente com o cliente. Nunca boletim genérico.

FORMATO DE SAÍDA — JSON puro, sem markdown, sem blocos de código:
{
  "titulo": "...",
  "subtitulo": "...",
  "categoria_principal": "Açúcar|Soja|Grãos|Mercado",
  "tags": ["açúcar", "soja", "exportação", "..."],
  "tempo_leitura_min": 7,
  "resumo_seo": "... (máx 160 chars)",
  "secoes": [
    {"titulo": "Açúcar: preços, spread e posição dos compradores", "conteudo": "..."},
    {"titulo": "Soja: cenário de exportação e demanda da China", "conteudo": "..."},
    {"titulo": "Câmbio e competitividade brasileira esta semana", "conteudo": "..."},
    {"titulo": "Quem está comprando — posição dos grandes players", "conteudo": "..."},
    {"titulo": "Como a GrandPacific opera neste cenário", "conteudo": "..."}
  ],
  "indicadores": [
    {"label": "Açúcar #11 (ICE)",    "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"},
    {"label": "Açúcar #5 (branco)",  "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"},
    {"label": "Spread #5/#11",       "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"},
    {"label": "Soja CBOT",           "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"},
    {"label": "Farelo de Soja",      "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"},
    {"label": "USD/BRL",             "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"}
  ],
  "call_to_action": "..."
}"""


# ── 7. GERAÇÃO VIA CLAUDE ─────────────────────────────────────────────────────

def generate_report(data: dict) -> Optional[dict]:
    """Chama Claude API e gera o relatório."""
    print("→ Gerando relatório com Claude API...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prices = data.get("precos", {})
    spread = data.get("sugar_spread", {})

    def fmt(key: str) -> str:
        p = prices.get(key, {})
        if not p:
            return "indisponível"
        return (
            f"{p.get('price','?')} {p.get('unit','')} "
            f"| semana: {p.get('var_5d',0):+.1f}% "
            f"| mês: {p.get('var_20d',0):+.1f}%"
        )

    user_prompt = f"""Gere o relatório semanal da GrandPacific — Semana {data['semana']}/{data['ano']}.

━━━ PREÇOS REAIS (Yahoo Finance / futuros CBOT & ICE) ━━━

AÇÚCAR (foco principal):
• Sugar #11 (ICE, bruto/raw):     {fmt('acucar_11')}
• Sugar #5 (Euronext, branco):    {fmt('acucar_5')}
• Spread #5 vs #11:               {json.dumps(spread, ensure_ascii=False)}

SOJA (foco principal):
• Soja em grão (CBOT ZS=F):       {fmt('soja')}
• Farelo de soja (CBOT ZM=F):     {fmt('soja_meal')}
• Óleo de soja (CBOT ZL=F):       {fmt('soja_oil')}

CONTEXTO:
• Milho (CBOT ZC=F):              {fmt('milho')}
• Trigo (CBOT ZW=F):              {fmt('trigo')}
• Café Arábica (ICE KC=F):        {fmt('cafe')}
• USD/BRL:                        {fmt('usd_brl')}
• USD Index (DXY):                {fmt('usd_idx')}

━━━ EXPORTAÇÕES BRASILEIRAS (MDIC/Comex Stat — dados oficiais) ━━━
{json.dumps(data.get('exportacoes', {}), ensure_ascii=False, indent=2)}

━━━ COMPRADORES GLOBAIS ━━━
{json.dumps(data.get('compradores', {}), ensure_ascii=False, indent=2)}

━━━ CLIMA NAS REGIÕES PRODUTORAS ━━━
{chr(10).join('• ' + c for c in data.get('clima', [])) or '• Dados climáticos indisponíveis'}

INSTRUÇÃO FINAL: Use APENAS os dados acima. Nunca invente valores.
Retorne APENAS o JSON, sem texto adicional, sem markdown."""

    try:
        message = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=5000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        report = json.loads(raw)
        print("   ✓ Relatório gerado com sucesso")
        return report
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON parse: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] Claude API: {e}")
        return None


# ── 8. PUBLICAÇÃO NO SANITY ────────────────────────────────────────────────────

def clean_slug(title: str, week: int, year: int) -> str:
    """Gera slug limpo e único."""
    nfkd      = unicodedata.normalize("NFKD", title.lower())
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    clean     = re.sub(r"[^a-z0-9\s-]", "", ascii_str)
    slug      = re.sub(r"\s+", "-", clean.strip())[:55]
    return f"semana-{week}-{year}-{slug}"


def build_portable_text(secoes: list) -> list:
    """Converte seções em Portable Text."""
    blocks = []
    for secao in secoes:
        key = re.sub(r"[^a-z0-9]", "", secao["titulo"].lower())[:12]
        blocks.append({
            "_type": "block", "_key": f"h_{key}",
            "style": "h2",
            "children": [{"_type": "span", "text": secao["titulo"], "marks": []}],
            "markDefs": [],
        })
        for i, para in enumerate(secao["conteudo"].split("\n\n")):
            if para.strip():
                blocks.append({
                    "_type": "block", "_key": f"p_{key}_{i}",
                    "style": "normal",
                    "children": [{"_type": "span", "text": para.strip(), "marks": []}],
                    "markDefs": [],
                })
    return blocks


def publish_to_sanity(report: dict, data: dict) -> bool:
    """Publica no Sanity CMS."""
    print("→ Publicando no Sanity CMS...")
    week = data["semana"]
    year = data["ano"]
    slug = clean_slug(report.get("titulo", "relatorio"), week, year)

    document = {
        "_type":              "relatorioSemanal",
        "_id":                f"report-semana-{week}-{year}",
        "titulo":             report.get("titulo", ""),
        "subtitulo":          report.get("subtitulo", ""),
        "slug":               {"_type": "slug", "current": slug},
        "semana":             week,
        "ano":                year,
        "dataPublicacao":     datetime.now().date().isoformat(),
        "categoriaPrincipal": report.get("categoria_principal", "Mercado"),
        "tags":               report.get("tags", []),
        "tempoLeituraMin":    report.get("tempo_leitura_min", 7),
        "resumoSeo":          report.get("resumo_seo", ""),
        "body":               build_portable_text(report.get("secoes", [])),
        "indicadores":        report.get("indicadores", []),
        "callToAction":       report.get("call_to_action", ""),
        "geradoPorIA":        True,
        "revisadoEditorial":  False,
        "publicado":          False,
    }

    url     = f"https://{SANITY_PROJECT_ID}.api.sanity.io/v2021-06-07/data/mutate/{SANITY_DATASET}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {SANITY_API_TOKEN}"}
    try:
        r = requests.post(url, json={"mutations": [{"createOrReplace": document}]}, headers=headers, timeout=15)
        r.raise_for_status()
        doc_id = r.json().get("results", [{}])[0].get("id", "?")
        print(f"   ✓ Publicado — ID: {doc_id} | slug: {slug}")
        return True
    except requests.HTTPError as e:
        print(f"[ERROR] Sanity {e.response.status_code}: {e.response.text}")
        return False
    except Exception as e:
        print(f"[ERROR] Sanity: {e}")
        return False


# ── 9. PIPELINE PRINCIPAL ──────────────────────────────────────────────────────

def run():
    data = collect_all_data()

    report = generate_report(data)
    if not report:
        print("[FATAL] Falha na geração. Abortando.")
        return False

    backup = f"/tmp/report_semana_{data['semana']}_{data['ano']}.json"
    with open(backup, "w", encoding="utf-8") as f:
        json.dump({"data": data, "report": report}, f, ensure_ascii=False, indent=2)
    print(f"→ Backup: {backup}")

    success = publish_to_sanity(report, data)
    print("\n" + ("✓ Pipeline concluído com sucesso!" if success else "✗ Pipeline com erros."))
    return success


if __name__ == "__main__":
    run()

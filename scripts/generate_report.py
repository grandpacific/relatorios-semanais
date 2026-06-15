"""
GrandPacific — Gerador de Relatório Semanal v2.1
Foco: Açúcar & Soja — preços, exportação, compradores globais
Fluxo: Coleta dados → Claude API → Sanity CMS
Fix v2.1: max_tokens aumentado para 6000 + retry com JSON repair
"""

import os, re, json, sys, unicodedata, requests
from datetime import datetime
from typing import Optional
import anthropic
import yfinance as yf

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
SANITY_PROJECT_ID = os.getenv("SANITY_PROJECT_ID", "")
SANITY_DATASET    = os.getenv("SANITY_DATASET", "production")
SANITY_API_TOKEN  = os.getenv("SANITY_TOKEN") or os.getenv("SANITY_API_TOKEN", "")
OPEN_WEATHER_KEY  = os.getenv("OPEN_WEATHER_KEY", "")


# ── 1. PREÇOS ────────────────────────────────────────────────────────────────

def get_futures_price(symbol: str, name: str, unit: str) -> dict:
    try:
        df = yf.Ticker(symbol).history(period="15d", interval="1d")
        if df.empty:
            return {}
        now   = round(float(df["Close"].iloc[-1]), 2)
        p5d   = round(float(df["Close"].iloc[-6] if len(df) >= 6 else df["Close"].iloc[0]), 2)
        p20d  = round(float(df["Close"].iloc[0]), 2)
        v5d   = round((now - p5d)  / p5d  * 100, 2) if p5d  else 0
        v20d  = round((now - p20d) / p20d * 100, 2) if p20d else 0
        arrow = "▲" if v5d > 0 else "▼"
        print(f"   ✓ {name}: {now} {unit} ({arrow}{abs(v5d):.1f}% 5d)")
        return {"name": name, "ticker": symbol, "price": now,
                "price_5d": p5d, "price_20d": p20d,
                "var_5d": v5d, "var_20d": v20d,
                "unit": unit, "date": df.index[-1].strftime("%Y-%m-%d")}
    except Exception as e:
        print(f"   [WARN] {name}: {e}")
        return {}


def get_all_prices() -> dict:
    print("→ Buscando preços (Yahoo Finance)...")
    return {
        "acucar_11": get_futures_price("SB=F",     "Sugar #11 (raw/bruto)",  "USd/lb"),
        "acucar_5":  get_futures_price("SF=F",     "Sugar #5 (white/branco)","USD/ton"),
        "soja":      get_futures_price("ZS=F",     "Soybeans",               "USd/bu"),
        "soja_meal": get_futures_price("ZM=F",     "Soybean Meal",           "USD/ton"),
        "soja_oil":  get_futures_price("ZL=F",     "Soybean Oil",            "USd/lb"),
        "milho":     get_futures_price("ZC=F",     "Corn",                   "USd/bu"),
        "trigo":     get_futures_price("ZW=F",     "Wheat",                  "USd/bu"),
        "cafe":      get_futures_price("KC=F",     "Coffee Arabica",         "USd/lb"),
        "usd_brl":   get_futures_price("USDBRL=X", "USD/BRL",                "BRL"),
        "usd_idx":   get_futures_price("DX-Y.NYB", "USD Index (DXY)",        "pts"),
    }


def calc_sugar_spread(prices: dict) -> dict:
    try:
        raw   = prices.get("acucar_11", {}).get("price", 0)
        white = prices.get("acucar_5",  {}).get("price", 0)
        if not raw or not white:
            return {}
        raw_ton = round(raw * 22.0462, 2)
        spread  = round(white - raw_ton, 2)
        signal  = ("buy_white" if spread > 100 else
                   "buy_white" if spread > 80  else
                   "neutral"   if spread > 60  else "buy_raw")
        interp  = ("muito favorável — margem de refinamento acima do histórico" if spread > 100 else
                   "favorável ao branco"  if spread > 80 else
                   "neutro — faixa histórica" if spread > 60 else
                   "comprimido — mercado prefere bruto")
        return {"raw_per_ton": raw_ton, "white_per_ton": white,
                "spread_usd_ton": spread, "signal": signal,
                "interpretation": interp}
    except:
        return {}


# ── 2. EXPORTAÇÕES (MDIC/Comex Stat) ─────────────────────────────────────────

def get_comex_export(ncm: str, name: str) -> dict:
    try:
        year  = datetime.now().year
        month = datetime.now().month
        url = (f"https://api-comexstat.mdic.gov.br/general?"
               f"flow=export&monthYear={year-1}-01,{year}-{month:02d}"
               f"&ncm={ncm}&groupBy=monthYear&totals=true")
        r = requests.get(url, timeout=12, headers={"Accept": "application/json"})
        if r.status_code != 200:
            return {"commodity": name, "note": "API indisponível"}
        rows = r.json().get("data", {}).get("list", [])
        if not rows:
            return {"commodity": name, "note": "Sem dados"}
        recent = sorted(rows, key=lambda x: x.get("monthYear", ""), reverse=True)[:3]
        return {
            "commodity": name, "ncm": ncm,
            "recent_months": [{"month": x.get("monthYear"), "fob_usd": x.get("metricFOB", 0),
                                "kg_net": x.get("metricKGLiquido", 0)} for x in recent],
            "source": "MDIC/Comex Stat",
        }
    except Exception as e:
        return {"commodity": name, "note": f"Erro: {e}"}


def get_export_data() -> dict:
    print("→ Buscando exportações (MDIC/Comex Stat)...")
    return {
        "acucar_bruto":    get_comex_export("17011300", "Açúcar VHP/bruto"),
        "acucar_refinado": get_comex_export("17019900", "Açúcar ICUMSA 45"),
        "soja_grao":       get_comex_export("12019000", "Soja em grão"),
        "farelo_soja":     get_comex_export("23040000", "Farelo de soja"),
    }


# ── 3. COMPRADORES GLOBAIS ────────────────────────────────────────────────────

def get_global_buyers(usd_brl: float) -> dict:
    print("→ Montando perfil de compradores globais...")
    nota = (f"USD/BRL {usd_brl} — câmbio depreciado favorece exportadores brasileiros."
            if usd_brl > 5.0 else
            f"USD/BRL {usd_brl} — câmbio apreciado comprime margens de exportação.")
    return {
        "acucar": {
            "brasil_posicao": "Maior exportador global (~50% do comércio mundial)",
            "compradores": [
                {"pais": "China",        "share_pct": 18, "nota": "Compras via COFCO. Reservas estratégicas influenciam timing."},
                {"pais": "Índia",        "share_pct": 12, "nota": "Safra doméstica volátil — quando há déficit, entra forte no mercado global."},
                {"pais": "Oriente Médio","share_pct": 15, "nota": "Arábia Saudita, EAU, Egito. Preferem ICUMSA 45. Demanda estável."},
                {"pais": "Bangladesh",   "share_pct": 8,  "nota": "Comprador consistente de bruto brasileiro via refinarias locais."},
                {"pais": "Indonésia",    "share_pct": 7,  "nota": "Governo controla via BULOG. Janelas de compra concentradas."},
            ],
        },
        "soja": {
            "brasil_posicao": "Maior exportador global (~50% das exportações mundiais)",
            "compradores": [
                {"pais": "China",         "share_pct": 65, "nota": "Compras via COFCO/SINOGRAIN. Ligado ao ciclo suinícola e avícola."},
                {"pais": "União Europeia","share_pct": 10, "nota": "EUDR — rastreabilidade é requisito crescente."},
                {"pais": "Tailândia",     "share_pct": 4,  "nota": "Hub regional ASEAN. Demanda crescente."},
            ],
        },
        "cambio_nota": nota,
    }


# ── 4. CLIMA ──────────────────────────────────────────────────────────────────

def get_climate() -> list:
    if not OPEN_WEATHER_KEY:
        return ["Dados climáticos não configurados (OPEN_WEATHER_KEY ausente)"]
    regions = [
        {"name": "Sorriso (MT) — soja",       "lat": -12.5483, "lon": -55.7219},
        {"name": "Ribeirão Preto (SP) — cana", "lat": -21.1699, "lon": -47.8096},
        {"name": "Londrina (PR) — soja/trigo", "lat": -23.3045, "lon": -51.1696},
        {"name": "Rio Verde (GO) — soja",      "lat": -17.7983, "lon": -50.9283},
    ]
    alerts = []
    for r in regions:
        try:
            resp = requests.get(
                f"https://api.openweathermap.org/data/2.5/weather"
                f"?lat={r['lat']}&lon={r['lon']}&appid={OPEN_WEATHER_KEY}&units=metric&lang=pt_br",
                timeout=8)
            d    = resp.json()
            desc = d.get("weather", [{}])[0].get("description", "")
            temp = d.get("main", {}).get("temp", 0)
            hum  = d.get("main", {}).get("humidity", 0)
            alerts.append(f"{r['name']}: {desc}, {temp:.0f}°C, umidade {hum}%")
        except Exception as e:
            print(f"   [WARN] Clima {r['name']}: {e}")
    return alerts or ["Dados climáticos indisponíveis"]


# ── 5. SYSTEM PROMPT ──────────────────────────────────────────────────────────

SYSTEM_PROMPT_PT = """Você é o analista-chefe da GrandPacific, empresa especializada em trading
de commodities agrícolas físicas no Brasil — foco em Açúcar (ICUMSA 45, VHP) e Soja.

REGRAS ABSOLUTAS:
1. Use APENAS os dados fornecidos. Nunca invente preços ou volumes.
2. Se um dado não estiver disponível, diga "dado não disponível esta semana".
3. Retorne APENAS JSON válido — sem markdown, sem texto antes ou depois.
4. Mantenha cada seção concisa (máx 3 parágrafos) para não truncar o JSON.

TÍTULO — REGRA CRÍTICA:
O título é a chamada principal do relatório. NUNCA use "Relatório Semanal GrandPacific — Semana XX".
O título deve ser uma manchete jornalística que capture O FATO MAIS RELEVANTE da semana,
usando os dados reais fornecidos. Exemplos do estilo correto:
• "Soja retoma os US$ 1.196 e spread do açúcar sinaliza compra de bruto — o que isso significa para quem exporta"
• "Açúcar #11 cai 2% na semana mas spread #5/#11 fecha em US$ 87/t — janela para ICUMSA 45 ainda aberta"
• "Dólar a R$ 5,04 amplia margem do exportador brasileiro enquanto China aumenta compras de soja"
• "Trigo despenca 6% e milho recua — mas açúcar VHP segura prêmio de exportação para o Brasil"
O subtítulo complementa com o contexto estratégico em 1 frase.

FORMATO DE SAÍDA — JSON puro e completo:
{
  "titulo": "MANCHETE IMPACTANTE com dados reais desta semana — nunca genérico",
  "subtitulo": "Contexto estratégico em 1 frase para exportadores e traders",
  "categoria_principal": "Açúcar|Soja|Mercado",
  "tags": ["açúcar", "soja", "exportação"],
  "tempo_leitura_min": 6,
  "resumo_seo": "... (máx 155 chars, inclua preços reais)",
  "secoes": [
    {"titulo": "Açúcar: preços, spread #11/#5 e janela de exportação", "conteudo": "... (máx 3 parágrafos com dados reais)"},
    {"titulo": "Soja: cenário de exportação e demanda global",          "conteudo": "..."},
    {"titulo": "Câmbio e competitividade brasileira esta semana",       "conteudo": "..."},
    {"titulo": "Quem está comprando — posição dos grandes players",     "conteudo": "..."},
    {"titulo": "Como a GrandPacific opera neste cenário",              "conteudo": "..."}
  ],
  "indicadores": [
    {"label": "Açúcar #11 (ICE)",  "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"},
    {"label": "Açúcar #5 (branco)","valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"},
    {"label": "Spread #5/#11",     "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"},
    {"label": "Soja CBOT",         "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"},
    {"label": "Farelo de Soja",    "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"},
    {"label": "USD/BRL",           "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estavel"}
  ],
  "call_to_action": "..."
}"""

SYSTEM_PROMPT_EN = """You are the chief analyst at GrandPacific, a Brazilian agricultural commodities
trading company focused on Sugar (ICUMSA 45, VHP, raw) and Soybeans (grain, meal, oil).

ABSOLUTE RULES:
1. Use ONLY the data provided. Never invent prices or volumes.
2. If data is unavailable, state "data not available this week".
3. Return ONLY valid JSON — no markdown, no text before or after.
4. Keep each section concise (max 3 paragraphs) to avoid truncation.

HEADLINE — CRITICAL RULE:
The title is the report's main hook. NEVER use "GrandPacific Weekly Report — Week XX".
Write a journalistic headline capturing THE MOST RELEVANT FACT of the week using real data.
Examples of the correct style:
• "Soybeans recover to $11.81/bu as Sugar #11/#5 spread signals raw buying opportunity for Brazilian exporters"
• "Sugar #11 drops 2% but white premium holds at $87/t — ICUMSA 45 export window still open"
• "BRL weakens to 5.04 as China accelerates soybean purchases — best margin week for Brazilian exporters in Q2"
• "Wheat -6%, Corn -4%: grain selloff masks Sugar VHP export premium holding firm from Santos"
The subtitle adds strategic context in 1 sentence.

OUTPUT FORMAT — pure valid JSON:
{
  "titulo": "IMPACTFUL HEADLINE with real data from this week — never generic",
  "subtitulo": "Strategic context in 1 sentence for exporters and traders",
  "categoria_principal": "Sugar|Soybeans|Market",
  "tags": ["sugar", "soybeans", "brazil exports"],
  "tempo_leitura_min": 6,
  "resumo_seo": "... (max 155 chars, include real prices)",
  "secoes": [
    {"titulo": "Sugar: prices, #11/#5 spread and export window",    "conteudo": "... (max 3 paragraphs with real data)"},
    {"titulo": "Soybeans: export outlook and global demand",        "conteudo": "..."},
    {"titulo": "FX and Brazilian competitiveness this week",        "conteudo": "..."},
    {"titulo": "Who is buying — positioning of major players",      "conteudo": "..."},
    {"titulo": "How GrandPacific operates in this scenario",        "conteudo": "..."}
  ],
  "indicadores": [
    {"label": "Sugar #11 (ICE)",    "valor": "...", "variacao": "...", "tendencia": "up|down|stable"},
    {"label": "Sugar #5 (white)",   "valor": "...", "variacao": "...", "tendencia": "up|down|stable"},
    {"label": "Spread #5/#11",      "valor": "...", "variacao": "...", "tendencia": "up|down|stable"},
    {"label": "Soybeans CBOT",      "valor": "...", "variacao": "...", "tendencia": "up|down|stable"},
    {"label": "Soybean Meal",       "valor": "...", "variacao": "...", "tendencia": "up|down|stable"},
    {"label": "USD/BRL",            "valor": "...", "variacao": "...", "tendencia": "up|down|stable"}
  ],
  "call_to_action": "..."
}"""

# Mantém SYSTEM_PROMPT como alias do PT para compatibilidade
SYSTEM_PROMPT = SYSTEM_PROMPT_PT


# ── 6. GERAÇÃO DO RELATÓRIO ───────────────────────────────────────────────────

def fmt_price(prices: dict, key: str) -> str:
    p = prices.get(key, {})
    if not p:
        return "indisponível"
    return f"{p.get('price','?')} {p.get('unit','')} ({p.get('var_5d',0):+.1f}% 5d / {p.get('var_20d',0):+.1f}% 20d)"


def try_parse_json(raw: str) -> Optional[dict]:
    """Tenta parsear JSON com repair básico de truncamento."""
    raw = raw.strip()
    # Remove blocos de código
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw).rstrip("`").strip()
    # Tentativa 1: direto
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    # Tentativa 2: encontra o JSON válido até onde for possível
    # Fecha chaves/colchetes abertos para recuperar JSON parcial
    opens  = raw.count("{") - raw.count("}")
    opens2 = raw.count("[") - raw.count("]")
    repaired = raw + ("]" * opens2) + ("}" * opens)
    try:
        return json.loads(repaired)
    except:
        return None


def generate_report(data: dict, lang: str = "pt") -> Optional[dict]:
    label  = "PT 🇧🇷" if lang == "pt" else "EN 🇺🇸"
    print(f"→ Gerando relatório com Claude API [{label}]...")
    client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prices  = data["precos"]
    spread  = data["sugar_spread"]
    system  = SYSTEM_PROMPT_PT if lang == "pt" else SYSTEM_PROMPT_EN
    intro   = f"Gere o relatório semanal da GrandPacific — Semana {data['semana']}/{data['ano']}." if lang == "pt" else f"Generate the GrandPacific weekly report — Week {data['semana']}/{data['ano']}."

    prompt = f"""{intro}

━━━ PREÇOS DE FUTUROS ━━━
AÇÚCAR:
• Sugar #11 (ICE, bruto):     {fmt_price(prices, 'acucar_11')}
• Sugar #5 (Euronext, branco):{fmt_price(prices, 'acucar_5')}
• Spread #5/#11:              {json.dumps(spread, ensure_ascii=False)}

SOJA:
• Soja em grão (CBOT):  {fmt_price(prices, 'soja')}
• Farelo de soja (CBOT):{fmt_price(prices, 'soja_meal')}
• Óleo de soja (CBOT):  {fmt_price(prices, 'soja_oil')}

CONTEXTO:
• Milho (CBOT):         {fmt_price(prices, 'milho')}
• Trigo (CBOT):         {fmt_price(prices, 'trigo')}
• Café Arábica (ICE):   {fmt_price(prices, 'cafe')}
• USD/BRL:              {fmt_price(prices, 'usd_brl')}
• DXY (USD Index):      {fmt_price(prices, 'usd_idx')}

━━━ EXPORTAÇÕES BRASILEIRAS (MDIC/Comex Stat) ━━━
{json.dumps(data['exportacoes'], ensure_ascii=False, indent=2)}

━━━ COMPRADORES GLOBAIS ━━━
{json.dumps(data['compradores'], ensure_ascii=False, indent=2)}

━━━ CLIMA NAS REGIÕES PRODUTORAS ━━━
{chr(10).join('• ' + c for c in data['clima'])}

━━━ INSTRUÇÕES ━━━
Use APENAS os dados acima. Mantenha cada seção concisa (máx 3 parágrafos).
Retorne APENAS o JSON completo e válido, sem texto adicional."""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=6000,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        raw    = msg.content[0].text
        report = try_parse_json(raw)
        if report:
            print("   ✓ Relatório gerado com sucesso")
            print(f"   Tokens usados: input={msg.usage.input_tokens}, output={msg.usage.output_tokens}")
        else:
            print(f"[ERROR] JSON inválido mesmo após repair. Primeiros 500 chars:")
            print(raw[:500])
        return report
    except Exception as e:
        print(f"[ERROR] Claude API: {e}")
        return None


# ── 7. PUBLICAÇÃO NO SANITY ───────────────────────────────────────────────────

def build_portable_text(secoes: list) -> list:
    blocks = []
    for s in secoes:
        key = re.sub(r"[^a-z0-9]", "", s["titulo"].lower())[:12]
        blocks.append({"_type": "block", "_key": f"h_{key}", "style": "h2",
                        "children": [{"_type": "span", "text": s["titulo"], "marks": []}],
                        "markDefs": []})
        for i, para in enumerate(s["conteudo"].split("\n\n")):
            if para.strip():
                blocks.append({"_type": "block", "_key": f"p_{key}_{i}", "style": "normal",
                                "children": [{"_type": "span", "text": para.strip(), "marks": []}],
                                "markDefs": []})
    return blocks


def publish_to_sanity(report: dict, data: dict, lang: str = 'pt') -> bool:
    print("→ Publicando no Sanity CMS...")
    week = data["semana"]
    year = data["ano"]

    nfkd  = unicodedata.normalize("NFKD", report.get("titulo", "relatorio").lower())
    slug  = re.sub(r"\s+", "-", re.sub(r"[^a-z0-9\s-]", "",
                   nfkd.encode("ascii", "ignore").decode()).strip())[:55]
    slug  = f"semana-{week}-{year}-{lang}-{slug}"

    doc = {
        "_type":              "relatorioSemanal",
        "_id":                f"report-semana-{week}-{year}-{lang}",
        "titulo":             report.get("titulo", ""),
        "subtitulo":          report.get("subtitulo", ""),
        "slug":               {"_type": "slug", "current": slug},
        "semana":             week,
        "ano":                year,
        "dataPublicacao":     datetime.now().date().isoformat(),
        "categoriaPrincipal": report.get("categoria_principal", "Mercado"),
        "tags":               report.get("tags", []),
        "tempoLeituraMin":    report.get("tempo_leitura_min", 6),
        "resumoSeo":          report.get("resumo_seo", ""),
        "body":               build_portable_text(report.get("secoes", [])),
        "indicadores":        report.get("indicadores", []),
        "callToAction":       report.get("call_to_action", ""),
        "idioma":             lang,
        "geradoPorIA":        True,
        "revisadoEditorial":  False,
    }
    url     = f"https://{SANITY_PROJECT_ID}.api.sanity.io/v2021-06-07/data/mutate/{SANITY_DATASET}"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {SANITY_API_TOKEN}"}
    try:
        r = requests.post(url, json={"mutations": [{"createOrReplace": doc}]},
                          headers=headers, timeout=20)
        r.raise_for_status()
        doc_id = r.json().get("results", [{}])[0].get("id", "?")
        print(f"   ✓ Publicado — id: {doc_id} | slug: {slug}")
        return True
    except requests.HTTPError as e:
        print(f"[ERROR] Sanity {e.response.status_code}: {e.response.text[:300]}")
        return False
    except Exception as e:
        print(f"[ERROR] Sanity: {e}")
        return False


# ── 8. PIPELINE PRINCIPAL ────────────────────────────────────────────────────

def run():
    print("\n" + "="*60)
    print("  GrandPacific — Coleta de Dados v2.2 (PT + EN)")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("="*60 + "\n")

    # Coleta dados uma vez — compartilhados entre os dois idiomas
    prices  = get_all_prices()
    spread  = calc_sugar_spread(prices)
    exports = get_export_data()
    usd_brl = prices.get("usd_brl", {}).get("price", 0)
    buyers  = get_global_buyers(usd_brl)
    climate = get_climate()

    week = datetime.now().isocalendar()[1]
    year = datetime.now().year

    data = {
        "semana": week, "ano": year,
        "data_geracao": datetime.now().isoformat(),
        "precos": prices, "sugar_spread": spread,
        "exportacoes": exports, "compradores": buyers, "clima": climate,
    }

    results = {}
    all_ok  = True

    for lang in ["pt", "en"]:
        print(f"\n{'─'*50}")
        label = "🇧🇷 PORTUGUÊS" if lang == "pt" else "🇺🇸 ENGLISH"
        print(f"  Gerando versão {label}")
        print(f"{'─'*50}")

        report = generate_report(data, lang=lang)
        if not report:
            print(f"[FATAL] Falha na geração [{lang.upper()}]. Abortando.")
            sys.exit(1)

        # Backup local
        backup = f"/tmp/report_semana_{week}_{year}_{lang}.json"
        with open(backup, "w", encoding="utf-8") as f:
            json.dump({"data": data, "report": report, "lang": lang},
                      f, ensure_ascii=False, indent=2)
        print(f"→ Backup [{lang.upper()}]: {backup}")
        print(f"→ Título: {report.get('titulo', '—')}")

        ok = publish_to_sanity(report, data, lang=lang)
        results[lang] = ok
        if not ok:
            all_ok = False

    # Também salva backup combinado (compatibilidade com artifact upload)
    backup_main = f"/tmp/report_semana_{week}_{year}.json"
    with open(backup_main, "w", encoding="utf-8") as f:
        json.dump({"data": data, "results": results}, f, ensure_ascii=False, indent=2)

    print("\n" + "="*60)
    print(f"  🇧🇷 PT: {'✓ OK' if results.get('pt') else '✗ ERRO'}")
    print(f"  🇺🇸 EN: {'✓ OK' if results.get('en') else '✗ ERRO'}")
    print("="*60)

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    run()

"""
GrandPacific — Gerador Automático de Relatórios Semanais
Fluxo: Coleta de dados → Claude API → Sanity CMS
"""

import os
import json
import requests
from datetime import datetime, timedelta
from typing import Optional
import anthropic

# ---------------------------------------------------------------------------
# CONFIGURAÇÃO — preencha no arquivo .env ou como variáveis de ambiente
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
SANITY_PROJECT_ID = os.getenv("SANITY_PROJECT_ID", "")
SANITY_DATASET    = os.getenv("SANITY_DATASET", "production")
SANITY_API_TOKEN  = os.getenv("SANITY_API_TOKEN", "")

# APIs de dados de mercado (gratuitas / open)
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")   # preços e câmbio
OPEN_WEATHER_KEY  = os.getenv("OPEN_WEATHER_KEY", "")    # clima regiões agrícolas

# ---------------------------------------------------------------------------
# 1. COLETA DE DADOS DE MERCADO
# ---------------------------------------------------------------------------

def get_fx_usd_brl() -> dict:
    """Cotação USD/BRL via Alpha Vantage (gratuito)."""
    try:
        url = (
            "https://www.alphavantage.co/query"
            f"?function=CURRENCY_EXCHANGE_RATE"
            f"&from_currency=USD&to_currency=BRL"
            f"&apikey={ALPHA_VANTAGE_KEY}"
        )
        r = requests.get(url, timeout=10)
        data = r.json()
        rate_info = data.get("Realtime Currency Exchange Rate", {})
        return {
            "rate": float(rate_info.get("5. Exchange Rate", 0)),
            "last_refreshed": rate_info.get("6. Last Refreshed", ""),
        }
    except Exception as e:
        print(f"[WARN] FX fetch failed: {e}")
        return {"rate": 0.0, "last_refreshed": ""}


def get_commodity_prices() -> dict:
    """
    Preços de commodities agrícolas.
    Alpha Vantage cobre futuros CBOT (SOYBEAN, CORN, WHEAT).
    """
    commodities = {
        "soja":   "SOYBEAN",
        "milho":  "CORN",
        "trigo":  "WHEAT",
    }
    results = {}
    for name, symbol in commodities.items():
        try:
            url = (
                "https://www.alphavantage.co/query"
                f"?function=COMMODITY&symbol={symbol}&interval=monthly"
                f"&apikey={ALPHA_VANTAGE_KEY}"
            )
            r = requests.get(url, timeout=10)
            data = r.json()
            series = data.get("data", [])
            if series:
                latest = series[0]
                previous = series[1] if len(series) > 1 else series[0]
                price_now  = float(latest.get("value", 0))
                price_prev = float(previous.get("value", 1))
                variation  = ((price_now - price_prev) / price_prev * 100) if price_prev else 0
                results[name] = {
                    "price": price_now,
                    "unit": "USD/bushel",
                    "variation_pct": round(variation, 2),
                    "date": latest.get("date", ""),
                }
        except Exception as e:
            print(f"[WARN] Commodity {name} fetch failed: {e}")
            results[name] = {"price": 0.0, "unit": "USD/bushel", "variation_pct": 0.0}
    return results


def get_climate_alerts() -> list[str]:
    """
    Alertas climáticos para as principais regiões agrícolas.
    Usa OpenWeatherMap — regiões: Sorriso/MT, Londrina/PR, Passo Fundo/RS.
    """
    regions = [
        {"name": "Sorriso (MT)",      "lat": -12.5483, "lon": -55.7219},
        {"name": "Londrina (PR)",      "lat": -23.3045, "lon": -51.1696},
        {"name": "Passo Fundo (RS)",   "lat": -28.2620, "lon": -52.4083},
        {"name": "Rio Verde (GO)",     "lat": -17.7983, "lon": -50.9283},
    ]
    alerts = []
    for region in regions:
        try:
            url = (
                f"https://api.openweathermap.org/data/2.5/weather"
                f"?lat={region['lat']}&lon={region['lon']}"
                f"&appid={OPEN_WEATHER_KEY}&units=metric&lang=pt_br"
            )
            r = requests.get(url, timeout=10)
            data = r.json()
            desc = data.get("weather", [{}])[0].get("description", "")
            temp = data.get("main", {}).get("temp", 0)
            rain = data.get("rain", {}).get("1h", 0)
            alerts.append(
                f"{region['name']}: {desc}, {temp:.0f}°C"
                + (f", chuva {rain}mm/h" if rain else "")
            )
        except Exception as e:
            print(f"[WARN] Climate {region['name']} failed: {e}")
    return alerts


def get_boi_gordo_price() -> dict:
    """
    Preço do boi gordo — indicativo via scraping público CEPEA/ESALQ.
    Fallback: retorna dado vazio para o Claude indicar "dado indisponível esta semana".
    """
    # CEPEA disponibiliza CSV público
    try:
        url = "https://www.cepea.esalq.usp.br/br/indicador/boi-gordo.aspx"
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        # Parseamento simplificado — o script completo usaria BeautifulSoup
        # Por ora retorna placeholder estruturado
        return {
            "price": 0.0,
            "unit": "R$/arroba",
            "source": "CEPEA/ESALQ",
            "note": "Verificar manualmente esta semana"
        }
    except Exception as e:
        print(f"[WARN] Boi gordo fetch failed: {e}")
        return {"price": 0.0, "unit": "R$/arroba", "note": "Indisponível"}


def collect_market_data() -> dict:
    """Agrega todos os dados de mercado da semana."""
    print("→ Coletando dados de mercado...")
    week_num = datetime.now().isocalendar()[1]
    year     = datetime.now().year

    data = {
        "semana": week_num,
        "ano": year,
        "data_geracao": datetime.now().isoformat(),
        "cambio": get_fx_usd_brl(),
        "commodities": get_commodity_prices(),
        "boi_gordo": get_boi_gordo_price(),
        "clima": get_climate_alerts(),
    }
    print(f"   ✓ Dados coletados para Semana {week_num}/{year}")
    return data


# ---------------------------------------------------------------------------
# 2. GERAÇÃO DO RELATÓRIO VIA CLAUDE
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Você é o analista-chefe da GrandPacific, empresa especializada em operações
de agronegócio no Brasil — trading, hedge, logística e financiamento para produtores rurais,
exportadores e investidores do setor.

Seu papel é redigir relatórios semanais de mercado que seguem RIGOROSAMENTE estas diretrizes:

IDENTIDADE EDITORIAL OBRIGATÓRIA:
1. SEMPRE conecte os dados de mercado à perspectiva do cliente da GrandPacific.
2. SEMPRE destaque como a GrandPacific atua para mitigar riscos diante do cenário descrito
   (câmbio, clima, logística, geopolítica). Nunca de forma genérica — seja específico ao cenário.
3. SEMPRE comunique como a empresa busca melhores condições de compra, venda ou hedge.
4. SEMPRE termine com a seção "Como a GrandPacific opera neste cenário" (2–3 parágrafos),
   com linguagem confiante, sem jargão excessivo, que reforça o diferencial da empresa.
5. NUNCA soe como um boletim genérico de commodities.
   Tom: advisor de confiança falando diretamente com o cliente.
6. Use dados concretos como base, mas a análise estratégica é o produto real.
7. Transmita controle e competência mesmo em cenários adversos.
8. Foco em impacto: cada seção deve ter um insight acionável, não apenas descrição.

DIFERENCIAL GRANDPACIFIC A REFORÇAR (varie o foco a cada semana, não repita o mesmo ângulo):
- Hedge estruturado com antecedência (não reage, antecipa)
- Acesso a linhas de crédito internacional e contrapartes qualificadas
- Condições que o mercado spot não oferece a operadores individuais
- Transparência operacional e relatórios frequentes
- Equipe com visão de longo prazo, não apenas da semana
- Expertise em logística e corredores de exportação brasileiros
- Relacionamento com compradores internacionais (China, Oriente Médio, Europa)

FORMATO DE SAÍDA: JSON puro, sem markdown, sem blocos de código.
Estrutura exata:
{
  "titulo": "...",
  "subtitulo": "...",
  "categoria_principal": "Grãos|Proteínas|Commodities|Logística|Clima|Mercado",
  "tags": ["...", "..."],
  "tempo_leitura_min": 5,
  "resumo_seo": "...",
  "secoes": [
    {
      "titulo": "Contexto da semana",
      "conteudo": "..."
    },
    {
      "titulo": "O que isso significa para o produtor e exportador",
      "conteudo": "..."
    },
    {
      "titulo": "Oportunidade ou risco da semana",
      "conteudo": "..."
    },
    {
      "titulo": "Como a GrandPacific opera neste cenário",
      "conteudo": "..."
    }
  ],
  "indicadores": [
    {"label": "Soja (CBOT)", "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estável"},
    {"label": "Milho (CBOT)", "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estável"},
    {"label": "Trigo (CBOT)", "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estável"},
    {"label": "Boi Gordo", "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estável"},
    {"label": "USD/BRL", "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estável"},
    {"label": "Frete Paranaguá", "valor": "...", "variacao": "...", "tendencia": "alta|baixa|estável"}
  ],
  "call_to_action": "..."
}"""


def generate_report(market_data: dict) -> Optional[dict]:
    """Chama a Claude API com os dados de mercado e retorna o relatório estruturado."""
    print("→ Gerando relatório com Claude API...")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    user_prompt = f"""Gere o relatório semanal da GrandPacific com base nos dados abaixo.

DADOS DE MERCADO — Semana {market_data['semana']}/{market_data['ano']}:

CÂMBIO USD/BRL:
- Taxa atual: {market_data['cambio']['rate']}
- Última atualização: {market_data['cambio']['last_refreshed']}

COMMODITIES AGRÍCOLAS:
{json.dumps(market_data['commodities'], ensure_ascii=False, indent=2)}

BOI GORDO:
{json.dumps(market_data['boi_gordo'], ensure_ascii=False, indent=2)}

CONDIÇÕES CLIMÁTICAS NAS PRINCIPAIS REGIÕES PRODUTORAS:
{chr(10).join('- ' + a for a in market_data['clima']) if market_data['clima'] else '- Dados climáticos indisponíveis esta semana'}

DATA DE GERAÇÃO: {market_data['data_geracao']}

Lembre-se: o relatório deve ter impacto real para quem está ou quer estar com a GrandPacific.
A seção "Como a GrandPacific opera neste cenário" é a mais importante — seja específico ao
cenário desta semana, não genérico. Retorne APENAS o JSON, sem nenhum texto adicional."""

    try:
        message = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4000,
            messages=[{"role": "user", "content": user_prompt}],
            system=SYSTEM_PROMPT,
        )

        raw = message.content[0].text.strip()
        # Remove possíveis blocos de código se o modelo incluir por engano
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        report = json.loads(raw)
        print("   ✓ Relatório gerado com sucesso")
        return report

    except json.JSONDecodeError as e:
        print(f"[ERROR] Falha ao parsear JSON do Claude: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] Falha na chamada ao Claude API: {e}")
        return None


# ---------------------------------------------------------------------------
# 3. PUBLICAÇÃO NO SANITY CMS
# ---------------------------------------------------------------------------

def slug_from_title(title: str, week: int, year: int) -> str:
    """Gera slug SEO-friendly a partir do título."""
    import re
    import unicodedata
    # Remove acentos
    nfkd = unicodedata.normalize("NFKD", title.lower())
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    # Remove caracteres especiais e substitui espaços
    clean = re.sub(r"[^a-z0-9\s-]", "", ascii_str)
    slug = re.sub(r"\s+", "-", clean.strip())[:60]
    return f"semana-{week}-{year}-{slug}"


def build_sanity_document(report: dict, market_data: dict) -> dict:
    """Constrói o documento no formato esperado pelo Sanity."""
    week = market_data["semana"]
    year = market_data["ano"]
    slug = slug_from_title(report["titulo"], week, year)

    # Converte seções para Portable Text (formato nativo do Sanity)
    portable_text_body = []
    for secao in report.get("secoes", []):
        # Título da seção
        portable_text_body.append({
            "_type": "block",
            "_key": f"h_{secao['titulo'][:8].replace(' ','')}",
            "style": "h2",
            "children": [{"_type": "span", "text": secao["titulo"], "marks": []}],
            "markDefs": [],
        })
        # Parágrafos do conteúdo
        for i, paragrafo in enumerate(secao["conteudo"].split("\n\n")):
            if paragrafo.strip():
                portable_text_body.append({
                    "_type": "block",
                    "_key": f"p_{secao['titulo'][:4]}_{i}",
                    "style": "normal",
                    "children": [{"_type": "span", "text": paragrafo.strip(), "marks": []}],
                    "markDefs": [],
                })

    document = {
        "_type": "relatorioSemanal",
        "_id": f"report-semana-{week}-{year}",
        "titulo": report["titulo"],
        "subtitulo": report.get("subtitulo", ""),
        "slug": {"_type": "slug", "current": slug},
        "semana": week,
        "ano": year,
        "dataPublicacao": datetime.now().date().isoformat(),
        "categoriaPrincipal": report.get("categoria_principal", "Mercado"),
        "tags": report.get("tags", []),
        "tempoLeituraMin": report.get("tempo_leitura_min", 5),
        "resumoSeo": report.get("resumo_seo", ""),
        "body": portable_text_body,
        "indicadores": report.get("indicadores", []),
        "callToAction": report.get("call_to_action", ""),
        "geradoPorIA": True,
        "revisadoEditorial": False,
    }
    return document


def publish_to_sanity(document: dict) -> bool:
    """Envia o documento ao Sanity via Mutations API."""
    print("→ Publicando no Sanity CMS...")

    url = (
        f"https://{SANITY_PROJECT_ID}.api.sanity.io/v2021-06-07"
        f"/data/mutate/{SANITY_DATASET}"
    )
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SANITY_API_TOKEN}",
    }
    payload = {
        "mutations": [
            {
                "createOrReplace": document
            }
        ]
    }

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=15)
        r.raise_for_status()
        result = r.json()
        doc_id = result.get("results", [{}])[0].get("id", "?")
        print(f"   ✓ Publicado no Sanity — documento: {doc_id}")
        return True
    except requests.HTTPError as e:
        print(f"[ERROR] Sanity HTTP error: {e.response.status_code} — {e.response.text}")
        return False
    except Exception as e:
        print(f"[ERROR] Falha ao publicar no Sanity: {e}")
        return False


# ---------------------------------------------------------------------------
# 4. ORQUESTRADOR PRINCIPAL
# ---------------------------------------------------------------------------

def run():
    """Pipeline completo: coleta → geração → publicação."""
    print("\n" + "="*60)
    print("  GrandPacific — Gerador de Relatório Semanal")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("="*60 + "\n")

    # Etapa 1: Coleta de dados
    market_data = collect_market_data()

    # Etapa 2: Geração com Claude
    report = generate_report(market_data)
    if not report:
        print("[FATAL] Falha na geração do relatório. Abortando.")
        return False

    # Etapa 3: Salva backup local
    backup_path = f"/tmp/report_semana_{market_data['semana']}_{market_data['ano']}.json"
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump({"market_data": market_data, "report": report}, f, ensure_ascii=False, indent=2)
    print(f"→ Backup salvo em {backup_path}")

    # Etapa 4: Publicação no Sanity
    document = build_sanity_document(report, market_data)
    success = publish_to_sanity(document)

    print("\n" + ("✓ Pipeline concluído com sucesso!" if success else "✗ Pipeline concluído com erros."))
    return success


if __name__ == "__main__":
    run()

# 📊 GrandPacific — Relatórios Semanais de Mercado

Pipeline automatizado de inteligência de mercado para commodities agrícolas — Açúcar, Soja, Café e Proteínas.

Gerado toda segunda-feira às 06h00 (BRT) via GitHub Actions → Claude AI → Sanity CMS → grandpacific.com.br

---

## 🗂️ Estrutura do Repositório

```
relatorios-semanais/
├── .github/
│   └── workflows/
│       └── weekly_report.yml       # Agendamento e execução automática (cron: segunda 06h)
├── scripts/
│   └── generate_report.py          # Script principal de geração do relatório
├── .env.example                    # Variáveis de ambiente necessárias
└── README.md
```

---

## ⚙️ Como Funciona

```
GitHub Actions (cron)
        │
        ▼
generate_report.py
        │
        ├── Yahoo Finance API ──────► Preços: SB=F, SF=F, ZS=F, ZM=F, ZL=F
        │                                     ZC=F, ZW=F, KC=F, USDBRL=X, DX-Y.NYB
        │
        ├── MDIC / Comex Stat ──────► Exportações brasileiras (açúcar, soja, café)
        │
        ├── Spread Cálculo ─────────► Sugar #5 vs #11 (USD/ton) → sinal buy_white/neutral/buy_raw
        │
        └── Claude API (Anthropic) ─► Análise narrativa + recomendações
                │
                ▼
        Sanity CMS (grandpacific-insights.sanity.studio)
                │
                ▼
        grandpacific.com.br/insights
```

---

## 📈 Commodities Monitoradas

| Ticker | Produto | Bolsa |
|--------|---------|-------|
| `SB=F` | Açúcar Bruto #11 | ICE New York |
| `SF=F` | Açúcar Branco #5 | ICE London |
| `ZS=F` | Soja (Grão) | CBOT Chicago |
| `ZM=F` | Farelo de Soja | CBOT Chicago |
| `ZL=F` | Óleo de Soja | CBOT Chicago |
| `ZC=F` | Milho | CBOT Chicago |
| `ZW=F` | Trigo | CBOT Chicago |
| `KC=F` | Café Arábica | ICE New York |
| `USDBRL=X` | Dólar / Real | Forex |
| `DX-Y.NYB` | Índice DXY | ICE |

### Spread Açúcar #5 / #11
O script calcula automaticamente o spread entre açúcar branco (LIFFE #5) e bruto (ICE #11), convertendo ambos para USD/tonelada métrica:

| Spread | Sinal | Interpretação |
|--------|-------|---------------|
| > $100/t | `buy_white` | Produção de branco muito favorável |
| $80–100/t | `buy_white` | Favorável ao branco |
| $60–80/t | `neutral` | Sem vantagem clara |
| < $60/t | `buy_raw` | Produção de bruto mais vantajosa |

---

## 🚀 Setup

### 1. Variáveis de Ambiente (GitHub Secrets)

Configure em **Settings → Secrets and variables → Actions**:

| Secret | Descrição |
|--------|-----------|
| `ANTHROPIC_API_KEY` | Chave da API Claude (console.anthropic.com) |
| `SANITY_PROJECT_ID` | ID do projeto Sanity (`qkd3v8aw`) |
| `SANITY_DATASET` | Dataset Sanity (`production`) |
| `SANITY_TOKEN` | Token de escrita do Sanity Studio |

### 2. Dependências Python

```bash
pip install anthropic requests python-dotenv yfinance
```

### 3. Execução Manual

```bash
cd scripts
python generate_report.py
```

---

## 📅 Agendamento

O workflow roda automaticamente toda **segunda-feira às 06h00 BRT** (09h00 UTC):

```yaml
on:
  schedule:
    - cron: '0 9 * * 1'   # Segunda-feira 09h UTC = 06h BRT
  workflow_dispatch:        # Permite execução manual pelo GitHub UI
```

> **Nota:** O GitHub desativa workflows agendados automaticamente após **60 dias de inatividade** do repositório. Para reativar, acesse a aba [Actions](../../actions) e clique em **"Enable workflow"**, ou faça qualquer commit no repositório.

---

## 🗄️ Sanity CMS

- **Studio:** [grandpacific-insights.sanity.studio](https://grandpacific-insights.sanity.studio)
- **Project ID:** `qkd3v8aw`
- **Schema:** `relatorioSemanal`
- **CORS:** `grandpacific.com.br` autorizado

### Campos do Schema

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `titulo` | string | Título do relatório |
| `dataPublicacao` | datetime | Data de publicação |
| `categoriaPrincipal` | string | Açúcar / Soja / Café / Proteínas |
| `resumoExecutivo` | text | Resumo em 3–5 parágrafos |
| `indicadores` | array | Preços, variações e sinais |
| `analiseAcucar` | text | Análise detalhada açúcar + spread |
| `analiseSoja` | text | Análise detalhada soja |
| `exportacoesBrasil` | object | Dados MDIC/Comex Stat |
| `recomendacoes` | array | Sinais e recomendações operacionais |

---

## 💰 Custo Estimado

| Item | Custo |
|------|-------|
| Relatório semanal (Claude API) | ~$0,50–1,00 / relatório |
| Alertas de mercado (quando ativados) | ~$0,30 / alerta |
| **Total mensal estimado** | **~$3–7 / mês** |

---

## 📁 Projetos Relacionados

| Projeto | Repositório / URL |
|---------|-------------------|
| Site institucional | grandpacific.com.br |
| Insights (WordPress) | grandpacific.com.br/insights |
| CRM Comercial | crm.grandpacific.com.br |
| Sanity Studio | grandpacific-insights.sanity.studio |

---

## 🏢 Sobre a GrandPacific Group

Trading de commodities agrícolas com escritórios em Nova York, Miami e São Paulo.

**Commodities:** Açúcar VHP · Açúcar ICUMSA 45 · Soja · Farelo · Café · Proteínas Animais

**Contato:** [grandpacific.com.br](https://grandpacific.com.br)

---

*Última atualização: maio 2026*

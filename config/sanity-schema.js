// schemas/relatorioSemanal.js
// Schema do Sanity para os relatórios semanais da GrandPacific

export default {
  name: "relatorioSemanal",
  title: "Relatório Semanal",
  type: "document",

  fields: [
    // ── Metadados ───────────────────────────────────────────────
    {
      name: "titulo",
      title: "Título",
      type: "string",
      validation: (R) => R.required().max(120),
    },
    {
      name: "subtitulo",
      title: "Subtítulo",
      type: "string",
      validation: (R) => R.max(200),
    },
    {
      name: "slug",
      title: "Slug (URL)",
      type: "slug",
      options: { source: "titulo", maxLength: 96 },
      validation: (R) => R.required(),
    },
    {
      name: "semana",
      title: "Número da semana",
      type: "number",
      validation: (R) => R.required().min(1).max(53),
    },
    {
      name: "ano",
      title: "Ano",
      type: "number",
      validation: (R) => R.required().min(2024),
    },
    {
      name: "dataPublicacao",
      title: "Data de publicação",
      type: "date",
      validation: (R) => R.required(),
    },
    {
      name: "categoriaP rincipal",
      title: "Categoria principal",
      type: "string",
      options: {
        list: [
          { title: "Grãos",        value: "Graos" },
          { title: "Proteínas",    value: "Proteinas" },
          { title: "Commodities",  value: "Commodities" },
          { title: "Logística",    value: "Logistica" },
          { title: "Clima",        value: "Clima" },
          { title: "Mercado",      value: "Mercado" },
        ],
        layout: "radio",
      },
      validation: (R) => R.required(),
    },
    {
      name: "tags",
      title: "Tags (SEO)",
      type: "array",
      of: [{ type: "string" }],
      options: { layout: "tags" },
    },
    {
      name: "tempoLeituraMin",
      title: "Tempo de leitura (min)",
      type: "number",
    },
    {
      name: "resumoSeo",
      title: "Resumo SEO (meta description)",
      type: "text",
      rows: 3,
      validation: (R) => R.max(160),
    },

    // ── Conteúdo principal ──────────────────────────────────────
    {
      name: "body",
      title: "Conteúdo do relatório",
      type: "array",
      of: [
        {
          type: "block",
          styles: [
            { title: "Normal", value: "normal" },
            { title: "H2",     value: "h2" },
            { title: "H3",     value: "h3" },
            { title: "Citação", value: "blockquote" },
          ],
          marks: {
            decorators: [
              { title: "Negrito",  value: "strong" },
              { title: "Itálico", value: "em" },
            ],
          },
        },
      ],
    },

    // ── Indicadores de mercado ──────────────────────────────────
    {
      name: "indicadores",
      title: "Indicadores de mercado",
      type: "array",
      of: [
        {
          type: "object",
          fields: [
            { name: "label",     title: "Indicador", type: "string" },
            { name: "valor",     title: "Valor",     type: "string" },
            { name: "variacao",  title: "Variação",  type: "string" },
            {
              name: "tendencia",
              title: "Tendência",
              type: "string",
              options: {
                list: [
                  { title: "Alta",    value: "alta" },
                  { title: "Baixa",   value: "baixa" },
                  { title: "Estável", value: "estavel" },
                ],
                layout: "radio",
              },
            },
          ],
          preview: {
            select: { title: "label", subtitle: "valor" },
          },
        },
      ],
    },

    // ── CTA e controle de publicação ────────────────────────────
    {
      name: "callToAction",
      title: "Call to action",
      type: "string",
      description: "Frase final de convite ao cliente",
    },
    {
      name: "geradoPorIA",
      title: "Gerado por IA",
      type: "boolean",
      initialValue: true,
      readOnly: true,
    },
    {
      name: "revisadoEditorial",
      title: "Revisado pela equipe editorial",
      type: "boolean",
      initialValue: false,
      description: "Marque após revisão antes de publicar",
    },
    {
      name: "publicado",
      title: "Publicado no site",
      type: "boolean",
      initialValue: false,
    },
  ],

  // ── Ordenação padrão no painel ──────────────────────────────
  orderings: [
    {
      title: "Mais recente",
      name: "dataPublicacaoDesc",
      by: [{ field: "dataPublicacao", direction: "desc" }],
    },
  ],

  // ── Preview no painel ───────────────────────────────────────
  preview: {
    select: {
      title:    "titulo",
      semana:   "semana",
      ano:      "ano",
      revisado: "revisadoEditorial",
      publicado: "publicado",
    },
    prepare({ title, semana, ano, revisado, publicado }) {
      const status = publicado ? "✓ Publicado" : revisado ? "⏳ Aguarda publicação" : "🤖 Gerado por IA";
      return {
        title,
        subtitle: `Semana ${semana}/${ano} — ${status}`,
      };
    },
  },
};

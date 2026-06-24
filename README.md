# 🇵🇪 Observatorio de Universidades del Perú

Ranking de investigación de las universidades peruanas (públicas y privadas) con **datos reales** de [OpenAlex](https://openalex.org) (CC0).

**🔗 En vivo:** https://unimauro.github.io/universidades-peru/

## Qué muestra
- **96 universidades** (públicas y privadas) con bibliometría real: publicaciones, citaciones, h-index, citas por publicación.
- **Índice de Investigación** (referencial, 0–100): h-index 40%, publicaciones 25%, citaciones 20%, citas/publicación 15%.
- Ranking completo **buscable, filtrable y ordenable**.
- Comparación **pública vs privada** y gráficos de volumen vs impacto.
- Asistente de IA para preguntar a los datos.

## Honestidad / limitaciones
- La bibliometría favorece áreas de alta citación (medicina, ciencias) sobre ingeniería o humanidades.
- Un solo paper muy citado puede inflar el h-index de una universidad pequeña.
- **No** mide docencia, empleabilidad ni infraestructura. Es un retrato de **producción científica**, no un ranking oficial.
- Se excluyen instituciones con menos de 30 publicaciones.

## Datos
`data/universidades.json` — generado desde la API de OpenAlex (`country_code:pe, type:education`).

## Proyecto hermano
🎓 [Hacia el Gemelo Digital de la UNI](https://unimauro.github.io/gemelo-digital-uni/) — observatorio + simulador de impacto de la Universidad Nacional de Ingeniería.

---
Creado por Carlos Cárdenas Fernández. Datos OpenAlex (CC0).

# ETL — actualización anual de datos

Regenera `data/universidades.json` (bibliometría OpenAlex + %Q1 SCImago + presupuesto MEF).

```bash
cd etl
python3 -m venv venv
venv/bin/pip install requests pandas pyarrow
venv/bin/python actualizar.py            # corrida completa (~5-10 min)
venv/bin/python actualizar.py --limit 5  # prueba rápida
```

Notas:
- Las respuestas de API se cachean en `etl/cache/` (borrar para forzar re-descarga).
- El respaldo del JSON anterior queda en `etl/cache/universidades.prev.json`.
- Para el siguiente año: subir `ANIO_PUBS`, `VENTANA`, `SJR_ANIO` y `MEF_RID`
  (resource id del año en datosabiertos.mef.gob.pe, dataset "presupuesto y ejecución de gasto")
  y el parquet `sjr_journals-<año+1>.parquet` de github.com/ikashnitsky/sjrdata.
- La lista de universidades sale del JSON existente: las exclusiones por perfiles
  contaminados (p. ej. Universidad San Pedro / USP São Paulo) se preservan.

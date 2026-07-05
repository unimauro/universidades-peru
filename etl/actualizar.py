#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ETL de actualización — Observatorio de Universidades del Perú.

Refresca data/universidades.json conservando la metodología publicada:
  - OpenAlex: bibliometría acumulada (works/citas/h/i10/mcit), serie anual
    (w23/w24/w25), ventana reciente (wr), campos temáticos, Nature&Science,
    núcleo investigador (autores ≥5 works con último afiliado en la uni).
  - SCImago SJR (vía parquet de github.com/ikashnitsky/sjrdata): %Q1 y %Q1-Q2
    cruzando por ISSN las revistas (primary_location.source) de cada uni.
  - MEF Datos Abiertos (SIAF): PIM + devengado por pliego y partida de
    investigación (Programa Presupuestal 0137 "Desarrollo de la CTI").
  - Índice de Investigación (idéntico al publicado): score = 100·(0.45·h/hmax
    + 0.25·min(cpp,p95)/p95 + 0.15·log1p(works)/log1p(wmax)
    + 0.15·log1p(cited)/log1p(cmax)), p95 con índice ceil.

La lista de universidades es la del JSON existente (exclusiones tipo
"Universidad San Pedro" se preservan: no se agregan perfiles nuevos a ciegas).

Uso:  python actualizar.py [--limit N] [--sin-cache]
Requiere venv con: requests pandas pyarrow  (ver etl/README.md)
"""
import argparse, hashlib, json, math, os, subprocess, sys, time
import urllib.parse

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DATA = os.path.join(ROOT, "data", "universidades.json")
CACHE = os.path.join(HERE, "cache")
MAILTO = "unimauro@gmail.com"

ANIO_PUBS = 2025          # último año cerrado de publicaciones
VENTANA = (2021, 2025)    # ventana reciente
SJR_ANIO = 2025           # SCImago Journal Rank
MEF_RID = "77fc3228-fa6f-4c1f-a0ed-d32520ad11ad"   # gasto 2025 (SIAF)
MEF_API = "https://api.datosabiertos.mef.gob.pe/DatosAbiertos/v1/datastore_search_sql"
SJR_PARQUET = ("https://raw.githubusercontent.com/ikashnitsky/sjrdata/master/"
               "data-raw/sjr-journal/sjr_journals-2026.parquet")
OA = "https://api.openalex.org"
MIN_WORKS_Q = 400         # umbral publicado para %Q1
ISSN_NS = "0028-0836|0036-8075"  # Nature, Science

FIELD_ES = {
    "Medicine": "Medicina", "Social Sciences": "Ciencias Sociales",
    "Computer Science": "Computación",
    "Economics, Econometrics and Finance": "Economía y Finanzas",
    "Agricultural and Biological Sciences": "Ciencias Agrarias y Biológicas",
    "Environmental Science": "Ciencias Ambientales",
    "Health Professions": "Profesiones de la Salud",
    "Business, Management and Accounting": "Negocios y Administración",
    "Psychology": "Psicología", "Arts and Humanities": "Artes y Humanidades",
    "Biochemistry, Genetics and Molecular Biology": "Bioquímica y Biología Molecular",
    "Immunology and Microbiology": "Inmunología y Microbiología",
    "Dentistry": "Odontología", "Nursing": "Enfermería",
    "Engineering": "Ingeniería", "Physics and Astronomy": "Física y Astronomía",
    "Materials Science": "Ciencia de Materiales", "Chemistry": "Química",
    "Earth and Planetary Sciences": "Ciencias de la Tierra",
    "Neuroscience": "Neurociencia", "Mathematics": "Matemáticas",
    "Decision Sciences": "Ciencias de la Decisión", "Energy": "Energía",
    "Pharmacology, Toxicology and Pharmaceutics": "Farmacología",
    "Chemical Engineering": "Ingeniería Química", "Veterinary": "Veterinaria",
    "Multidisciplinary": "Multidisciplinario",
}
TIPO_ES = {
    "article": "Artículos", "book-chapter": "Capítulos de libro",
    "review": "Revisiones", "preprint": "Preprints", "dataset": "Datasets",
    "dissertation": "Tesis", "book": "Libros",
}


def log(*a):
    print(*a, flush=True)


# ---------- HTTP con caché en disco ----------
def get_json(url, usar_cache=True, intentos=4):
    os.makedirs(CACHE, exist_ok=True)
    clave = hashlib.sha1(url.encode()).hexdigest() + ".json"
    ruta = os.path.join(CACHE, clave)
    if usar_cache and os.path.exists(ruta):
        with open(ruta) as f:
            return json.load(f)
    for i in range(intentos):
        try:
            r = requests.get(url, timeout=90,
                             headers={"User-Agent": f"observatorio-universidades-peru (mailto:{MAILTO})"})
            if r.status_code == 200:
                d = r.json()
                with open(ruta, "w") as f:
                    json.dump(d, f)
                time.sleep(0.12)
                return d
            if r.status_code in (429, 500, 502, 503):
                time.sleep(3 * (i + 1)); continue
            raise RuntimeError(f"HTTP {r.status_code}: {url[:140]}")
        except requests.RequestException as e:
            if i == intentos - 1:
                raise
            time.sleep(3 * (i + 1))
    raise RuntimeError(f"agotados reintentos: {url[:140]}")


def mef_sql(sql):
    """MEF datos abiertos vía curl (urllib/requests fallan con su WAF)."""
    url = MEF_API + "?sql=" + urllib.parse.quote(sql)
    clave = hashlib.sha1(url.encode()).hexdigest() + ".json"
    os.makedirs(CACHE, exist_ok=True)
    ruta = os.path.join(CACHE, clave)
    if os.path.exists(ruta):
        with open(ruta) as f:
            return json.load(f)["records"]
    out = subprocess.run(["curl", "-s", "--max-time", "180", url],
                         capture_output=True, text=True, check=True).stdout
    d = json.loads(out)
    if str(d.get("sucess", d.get("success"))).lower() != "true":
        raise RuntimeError(f"MEF: {out[:300]}")
    with open(ruta, "w") as f:
        json.dump(d, f)
    return d["records"]


# ---------- SJR ----------
def cargar_sjr():
    """dict issn(8 dígitos sin guion) -> best_quartile ('Q1'..'Q4') del SJR_ANIO."""
    import pandas as pd
    ruta = os.path.join(CACHE, "sjr_journals.parquet")
    if not os.path.exists(ruta):
        log(f"  descargando SJR parquet (~47MB)…")
        os.makedirs(CACHE, exist_ok=True)
        subprocess.run(["curl", "-sL", "--max-time", "600", "-o", ruta, SJR_PARQUET], check=True)
    df = pd.read_parquet(ruta)
    col_year = "year" if "year" in df.columns else next(c for c in df.columns if "year" in c.lower())
    df = df[df[col_year].astype(float).astype(int) == SJR_ANIO]
    col_q = next(c for c in df.columns if "quartile" in c.lower())
    col_issn = next(c for c in df.columns if c.lower() == "issn")
    m = {}
    for issn_raw, q in zip(df[col_issn], df[col_q]):
        q = str(q).strip().upper()
        if q not in ("Q1", "Q2", "Q3", "Q4"):
            continue
        for issn in str(issn_raw).replace(" ", "").split(","):
            issn = issn.strip().upper()
            if len(issn) == 8:
                # quedarse con el mejor cuartil si el ISSN se repite
                if issn not in m or q < m[issn]:
                    m[issn] = q
    log(f"  SJR {SJR_ANIO}: {len(m):,} ISSN con cuartil")
    return m


def issn8(s):
    return (s or "").replace("-", "").strip().upper()


# ---------- OpenAlex por universidad ----------
def datos_institucion(oa_id):
    d = get_json(f"{OA}/institutions/{oa_id}?mailto={MAILTO}")
    cy = {y["year"]: y["works_count"] for y in d.get("counts_by_year", [])}
    topics = d.get("topics", [])
    campos = {}
    for t in topics:
        f = FIELD_ES.get(t["field"]["display_name"], t["field"]["display_name"])
        campos[f] = campos.get(f, 0) + t["count"]
    tot = sum(campos.values())
    fdist = [{"f": f, "pct": round(100 * n / tot, 1)}
             for f, n in sorted(campos.items(), key=lambda x: -x[1])[:10]] if tot else []
    return {
        "works": d["works_count"], "cited": d["cited_by_count"],
        "h": d["summary_stats"]["h_index"], "i10": d["summary_stats"]["i10_index"],
        "mcit": round(d["summary_stats"]["2yr_mean_citedness"], 2),
        "w23": cy.get(2023, 0), "w24": cy.get(2024, 0), "w25": cy.get(2025, 0),
        "wr": sum(cy.get(y, 0) for y in range(VENTANA[0], VENTANA[1] + 1)),
        "area": topics[0]["display_name"] if topics else None,
        "toptopics": [t["display_name"] for t in topics[:6]],
        "fdist": fdist,
        "fields": [{"f": x["f"], "pct": int(round(x["pct"]))} for x in fdist[:3]],
        "field": fdist[0]["f"] if fdist else "Sin datos",
        "_campos_n": campos,
    }


def contar_ns(oa_id):
    d = get_json(f"{OA}/works?filter=institutions.id:{oa_id},"
                 f"primary_location.source.issn:{ISSN_NS}&per-page=1&mailto={MAILTO}")
    return d["meta"]["count"]


def contar_investigadores(oa_id):
    d = get_json(f"{OA}/authors?filter=last_known_institutions.id:{oa_id},"
                 f"works_count:%3E4&per-page=1&mailto={MAILTO}")
    return d["meta"]["count"]


def fuentes_uni(oa_id):
    """[(source_id, n_works)] top 200 revistas de la uni."""
    d = get_json(f"{OA}/works?filter=institutions.id:{oa_id}"
                 f"&group_by=primary_location.source.id&per-page=200&mailto={MAILTO}")
    out = []
    for g in d.get("group_by", []):
        k = g.get("key") or ""
        if k.startswith("https://openalex.org/S"):
            out.append((k.rsplit("/", 1)[1], g["count"]))
    return out


def resolver_issn(source_ids, cache_issn):
    """Rellena cache_issn[source_id] = [issn8, ...] en lotes de 50."""
    faltan = [s for s in source_ids if s not in cache_issn]
    for i in range(0, len(faltan), 50):
        lote = faltan[i:i + 50]
        d = get_json(f"{OA}/sources?filter=ids.openalex:{'|'.join(lote)}"
                     f"&per-page=50&select=id,issn_l,issn&mailto={MAILTO}")
        for s in d.get("results", []):
            sid = s["id"].rsplit("/", 1)[1]
            issns = set()
            if s.get("issn_l"):
                issns.add(issn8(s["issn_l"]))
            for x in s.get("issn") or []:
                issns.add(issn8(x))
            cache_issn[sid] = [x for x in issns if len(x) == 8]
        for s in lote:
            cache_issn.setdefault(s, [])


def calidad_q(fuentes, cache_issn, sjr):
    """(%Q1, %Q1-Q2, n_revistas_matched).

    Denominador = TODAS las publicaciones con revista identificada (tengan o
    no cuartil SJR); así una revista fuera de SJR (baja indexación) cuenta en
    contra, igual que en la versión publicada."""
    denom = q1 = q12 = rev = 0
    for sid, n in fuentes:
        denom += n
        qs = [sjr[i] for i in cache_issn.get(sid, []) if i in sjr]
        if not qs:
            continue
        q = min(qs)  # mejor cuartil
        rev += 1
        if q == "Q1":
            q1 += n
        if q in ("Q1", "Q2"):
            q12 += n
    if not denom:
        return None, None, 0
    return round(100 * q1 / denom, 1), round(100 * q12 / denom, 1), rev


# ---------- MEF ----------
def presupuesto_mef():
    """pliego -> {pim, dev} en millones; y pliego -> idi (devengado PP 0137, millones)."""
    recs = mef_sql(f'SELECT "PLIEGO",MAX("PLIEGO_NOMBRE") nom,'
                   f'SUM("MONTO_PIM"::numeric) pim,SUM("MONTO_DEVENGADO"::numeric) dev '
                   f'FROM "{MEF_RID}" WHERE "SECTOR"=\'10\' GROUP BY 1')
    pres = {r["PLIEGO"].lstrip("0"): {"pim": round(float(r["pim"]) / 1e6, 1),
                                      "dev": round(float(r["dev"]) / 1e6, 1),
                                      "nom": r["nom"]}
            for r in recs}
    recs = mef_sql(f'SELECT "PLIEGO",SUM("MONTO_PIM"::numeric) pim,'
                   f'SUM("MONTO_DEVENGADO"::numeric) dev FROM "{MEF_RID}" '
                   f'WHERE "SECTOR"=\'10\' AND "PROGRAMA_PPTO"=\'0137\' GROUP BY 1')
    idi = {r["PLIEGO"].lstrip("0"): {"pim": round(float(r["pim"]) / 1e6, 2),
                                     "dev": round(float(r["dev"]) / 1e6, 2)}
           for r in recs}
    return pres, idi


# ---------- Índice ----------
def calcular_scores(unis):
    hmax = max(u["h"] for u in unis)
    wmax = max(u["works"] for u in unis)
    cmax = max(u["cited"] for u in unis)
    cpps = sorted(u["cpp"] for u in unis)
    p95 = cpps[math.ceil((len(cpps) - 1) * 0.95)]
    for u in unis:
        u["score"] = round(100 * (
            0.45 * u["h"] / hmax
            + 0.25 * min(u["cpp"], p95) / p95
            + 0.15 * math.log1p(u["works"]) / math.log1p(wmax)
            + 0.15 * math.log1p(u["cited"]) / math.log1p(cmax)), 1)
    for i, u in enumerate(sorted(unis, key=lambda x: -x["score"]), 1):
        u["rank"] = i
    return p95


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="solo N universidades (prueba)")
    ap.add_argument("--sin-cache", action="store_true")
    args = ap.parse_args()

    with open(DATA) as f:
        base = json.load(f)
    unis_base = base["unis"][:args.limit] if args.limit else base["unis"]
    log(f"Universidades: {len(unis_base)}")

    log("1) SJR…")
    sjr = cargar_sjr()

    log("2) MEF 2025 (PIM+devengado por pliego, PP 0137)…")
    pres, idi_map = presupuesto_mef()
    log(f"  pliegos sector 10: {len(pres)}; con PP 0137: {len(idi_map)}")

    log("3) OpenAlex por universidad…")
    cache_issn, nuevos = {}, []
    for i, b in enumerate(unis_base, 1):
        oa_id = b["oa"].rsplit("/", 1)[1]
        u = {k: b[k] for k in ("k", "name", "acron", "tipo", "city", "lat", "lng", "oa", "ror") if k in b}
        if "pliego" in b:
            u["pliego"] = b["pliego"]
        try:
            u.update(datos_institucion(oa_id))
        except Exception as e:
            log(f"  !! {b['acron']}: institución falló ({e}); conservo valores previos")
            u.update({k: b.get(k) for k in ("works", "cited", "h", "i10", "mcit", "w23", "w24",
                                            "wr", "area", "toptopics", "fdist", "fields", "field")})
            u["w25"] = 0
            u["_campos_n"] = {}
        u["cpp"] = round(u["cited"] / u["works"], 1) if u["works"] else 0.0
        u["ns"] = contar_ns(oa_id)
        u["inv"] = contar_investigadores(oa_id)
        if u["inv"]:
            u["pub_inv"] = round(u["works"] / u["inv"], 1)
            u["cit_inv"] = round(u["cited"] / u["inv"])
        if u["works"] >= MIN_WORKS_Q:
            fs = fuentes_uni(oa_id)
            resolver_issn([s for s, _ in fs], cache_issn)
            q1, q12, nrev = calidad_q(fs, cache_issn, sjr)
            if q1 is not None:
                u["q1_pct"], u["q12_pct"] = q1, q12
                u["_nrev"] = nrev
        # presupuesto
        if u.get("pliego"):
            p = pres.get(u["pliego"].lstrip("0"))
            if p:
                u["pim"], u["dev"] = p["pim"], p["dev"]
            ii = idi_map.get(u["pliego"].lstrip("0"))
            u["idi"] = ii["pim"] if ii else 0          # partida CTI asignada (PIM)
            u["idi_dev"] = ii["dev"] if ii else 0      # partida CTI ejecutada
            u["idi_pct"] = round(100 * u["idi"] / u["pim"], 2) if ii and u.get("pim") else 0
            if u["idi"] and u["w25"]:
                u["cost_idi"] = round(u["idi"] * 1e6 / u["w25"])
                u["eff_idi"] = round(u["w25"] / u["idi"], 1)
            else:
                u["cost_idi"] = u["eff_idi"] = None
        nuevos.append(u)
        if i % 10 == 0 or i == len(unis_base):
            log(f"  {i}/{len(unis_base)} ({u['acron']})")

    # claves únicas: el JSON heredado traía k duplicados (unap: Amazonía/Altiplano,
    # unt: Trujillo/Tumbes) que colisionaban en el comparador y en el %Q1 viejo
    vistos = {}
    for u in nuevos:
        if u["k"] in vistos:
            sufijo = "".join(c for c in u["name"].split()[-1].lower() if c.isalnum())
            u["k"] = u["k"] + "-" + sufijo
            log(f"  k duplicado → {u['k']} ({u['name'][:40]})")
        vistos[u["k"]] = True

    log("4) Índice y meta…")
    p95 = calcular_scores(nuevos)

    # tipos de documento (una consulta agrupada, en 2 lotes)
    ids = [b["oa"].rsplit("/", 1)[1] for b in unis_base]
    tipos_n = {}
    for i in range(0, len(ids), 50):
        d = get_json(f"{OA}/works?filter=institutions.id:{'|'.join(ids[i:i+50])}"
                     f"&group_by=type&per-page=50&mailto={MAILTO}")
        for g in d.get("group_by", []):
            tipos_n[g["key"]] = tipos_n.get(g["key"], 0) + g["count"]
    tipos = {}
    for k, n in tipos_n.items():
        tipos[TIPO_ES.get(k, "Otros")] = tipos.get(TIPO_ES.get(k, "Otros"), 0) + n
    tipos = [{"t": t, "n": n} for t, n in sorted(tipos.items(), key=lambda x: -x[1])]

    campos_tot = {}
    for u in nuevos:
        for f, n in u.pop("_campos_n", {}).items():
            campos_tot[f] = campos_tot.get(f, 0) + n
    nrev_tot = sum(u.pop("_nrev", 0) for u in nuevos)

    meta = dict(base["meta"])
    con_pres = [u for u in nuevos if u.get("pim")]
    meta.update({
        "extraido": "2026-07",
        "total": len(nuevos),
        "n_publicas": sum(1 for u in nuevos if u["tipo"] == "Pública"),
        "n_privadas": sum(1 for u in nuevos if u["tipo"] == "Privada"),
        "n_con_presupuesto": len(con_pres),
        "presupuesto_total_publicas": round(sum(u["pim"] for u in con_pres), 1),
        "devengado_total_publicas": round(sum(u.get("dev", 0) for u in con_pres), 1),
        "campos": [{"f": f, "n": n} for f, n in sorted(campos_tot.items(), key=lambda x: -x[1])],
        "areas_filtro": [f for f, _ in sorted(
            ((f, sum(1 for u in nuevos if u["field"] == f)) for f in {u["field"] for u in nuevos if u["field"] != "Sin datos"}),
            key=lambda t: -t[1])][:12],
        "tipos": tipos,
        "ns_total": sum(u["ns"] for u in nuevos),
        "ns_unis": sum(1 for u in nuevos if u["ns"] > 0),
        "idi_total": round(sum(u.get("idi") or 0 for u in nuevos), 1),
        "idi_dev_total": round(sum(u.get("idi_dev") or 0 for u in nuevos), 1),
        "ventana": ("Índice y totales = HISTÓRICO/ACUMULADO (toda la trayectoria indexada, ~hasta 2026). "
                    f"La columna \"Public. {VENTANA[0]}-{VENTANA[1]}\" da una ventana reciente."),
        "nota": ("Bibliometría real. Índice de Investigación referencial PONDERADO HACIA IMPACTO "
                 "(h-index 45%, citas/publicación 25% con tope percentil 95, volumen de publicaciones 15% "
                 "y citaciones 15%) para resistir la inflación por revistas predadoras. "
                 f"Eficiencia (I+D) = publicaciones {ANIO_PUBS} por cada S/ millón de la partida de "
                 f"investigación (PP 0137, PIM {ANIO_PUBS}); ambos del mismo año cerrado."),
        "calidad_q": (f"% de publicaciones en revistas Q1/Q1-Q2 según SCImago Journal Rank {SJR_ANIO} "
                      "(cruzado por ISSN con las revistas de cada universidad en OpenAlex). "
                      "Cobertura: universidades con ≥400 publicaciones."),
        "fuentes_calidad": f"SCImago Journal Rank {SJR_ANIO} (vía github.com/ikashnitsky/sjrdata) + OpenAlex",
        "idi_metodologia": ("El costo por publicación y la eficiencia se calculan con la PARTIDA DE INVESTIGACIÓN "
                            f"(programa \"Desarrollo de la CTI\" 0137, PIM MEF/SIAF {ANIO_PUBS}), "
                            "NO con el presupuesto total (que incluye sueldos, servicios, etc.). "
                            f"Publicaciones del mismo año cerrado ({ANIO_PUBS}). Se incluye además la EJECUCIÓN "
                            "(devengado) de esa partida: varias universidades ejecutan una fracción mínima de lo asignado."),
        "percapita": dict(meta["percapita"]) if isinstance(meta.get("percapita"), dict) else meta.get("percapita"),
        "presupuesto_nota": (f"PIM y DEVENGADO {ANIO_PUBS} (año cerrado, MEF/SIAF Datos Abiertos) por pliego; "
                             "solo universidades públicas tienen pliego presupuestal."),
    })
    meta["cobertura"] = meta["cobertura"].replace("PIM 2025", f"PIM/devengado {ANIO_PUBS}")

    salida = {"unis": nuevos, "meta": meta}
    prev = os.path.join(CACHE, "universidades.prev.json")
    os.replace(DATA, prev)
    with open(DATA, "w") as f:
        json.dump(salida, f, ensure_ascii=False, separators=(",", ":"))
    log(f"LISTO ✓  {DATA}  ({os.path.getsize(DATA)//1024} KB; respaldo previo en {prev})")
    log(f"  p95 citas/pub = {p95} | revistas SJR matched (suma por uni): {nrev_tot}")


if __name__ == "__main__":
    main()

import argparse
import json
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


# En Docker/Portainer conviene que el "directorio de trabajo" (donde están los excels)
# sea configurable (ej: /opt/patagonia-ubicaciones montado a /data).
BASE_DIR = Path(__file__).resolve().parent


def _elementos_path() -> Path:
    return BASE_DIR / "elementos.xlsx"


def _processed_state_path() -> Path:
    return BASE_DIR / ".procesados.json"


def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _norm_col(s: str) -> str:
    s = str(s).strip().lower()
    s = _strip_accents(s)
    # algunos excels/exports muestran caracteres raros en consola; normalizamos lo más posible
    s = s.replace("�", "")
    s = " ".join(s.split())
    return s


def _pick_column(cols: list[str], want: str) -> str:
    """
    Devuelve el nombre real de columna que corresponde a `want` (codigo/color/talle/ubicacion).
    """
    norm_map = {c: _norm_col(c) for c in cols}
    # match exacto por preferencia
    for c, n in norm_map.items():
        if n == want:
            return c

    # heurísticas
    if want == "codigo":
        for c, n in norm_map.items():
            if "cod" in n:
                return c
    if want == "ubicacion":
        for c, n in norm_map.items():
            if "ubic" in n:
                return c
    if want == "color":
        for c, n in norm_map.items():
            if n == "color" or "color" in n:
                return c
    if want == "talle":
        for c, n in norm_map.items():
            if "talle" in n:
                return c

    raise SystemExit(f"No pude encontrar columna '{want}'. Columnas disponibles: {cols}")


@dataclass(frozen=True)
class Lookup:
    df: pd.DataFrame
    k_codigo: str
    k_item: str | None
    k_color: str
    k_talle: str
    v_ubicacion: str


def load_elementos_lookup() -> Lookup:
    elementos_xlsx = _elementos_path()
    if not elementos_xlsx.exists():
        raise SystemExit(f"No existe {elementos_xlsx}")

    df = pd.read_excel(elementos_xlsx, sheet_name="elementos", dtype=str, keep_default_na=False)
    cols = df.columns.tolist()

    k_codigo = _pick_column(cols, "codigo")
    # En algunos archivos de entrada, "codigo" en realidad es el "Item".
    # Si existe la columna Item, soportamos match alternativo por Item+Color+Talle.
    k_item = None
    for c in cols:
        if _norm_col(c) == "item":
            k_item = c
            break
    k_color = _pick_column(cols, "color")
    k_talle = _pick_column(cols, "talle")
    v_ubicacion = _pick_column(cols, "ubicacion")

    # normalizar keys para match consistente
    norm_cols = [k_codigo, k_color, k_talle, v_ubicacion] + ([k_item] if k_item else [])
    for c in norm_cols:
        df[c] = df[c].astype(str).str.strip()

    keep = [k_codigo, k_color, k_talle, v_ubicacion] + ([k_item] if k_item else [])
    df = df[keep].copy()
    # si hay duplicados, nos quedamos con la primera ubicación no vacía
    df = df[df[v_ubicacion].astype(str).str.strip().ne("")]
    df = df.drop_duplicates(subset=[k_codigo, k_color, k_talle], keep="first")

    if k_item:
        df = df.drop_duplicates(subset=[k_item, k_color, k_talle], keep="first")

    return Lookup(df=df, k_codigo=k_codigo, k_item=k_item, k_color=k_color, k_talle=k_talle, v_ubicacion=v_ubicacion)


def completar_archivo(xlsx_path: Path, lookup: Lookup, overwrite: bool) -> Path:
    df_in = pd.read_excel(xlsx_path, dtype=str, keep_default_na=False)
    cols = df_in.columns.tolist()

    c_codigo = _pick_column(cols, "codigo")
    c_color = _pick_column(cols, "color")
    c_talle = _pick_column(cols, "talle")
    c_ubicacion = _pick_column(cols, "ubicacion")

    for c in (c_codigo, c_color, c_talle, c_ubicacion):
        df_in[c] = df_in[c].astype(str).str.strip()

    df_lu_codigo = lookup.df.rename(
        columns={
            lookup.k_codigo: c_codigo,
            lookup.k_color: c_color,
            lookup.k_talle: c_talle,
            lookup.v_ubicacion: "__ubicacion_lookup",
        }
    )

    merged = df_in.merge(
        df_lu_codigo[[c_codigo, c_color, c_talle, "__ubicacion_lookup"]],
        on=[c_codigo, c_color, c_talle],
        how="left",
    )
    merged["__ubicacion_lookup"] = merged["__ubicacion_lookup"].fillna("")
    # Si no hubo match por Código, intentamos match por Item (cuando el Excel de entrada usa "CODIGO" = Item).
    if lookup.k_item:
        df_lu_item = lookup.df.rename(
            columns={
                lookup.k_item: c_codigo,
                lookup.k_color: c_color,
                lookup.k_talle: c_talle,
                lookup.v_ubicacion: "__ubicacion_lookup_item",
            }
        )
        merged = merged.merge(
            df_lu_item[[c_codigo, c_color, c_talle, "__ubicacion_lookup_item"]],
            on=[c_codigo, c_color, c_talle],
            how="left",
        )
        merged["__ubicacion_lookup_item"] = merged["__ubicacion_lookup_item"].fillna("")
        merged["__ubicacion_lookup"] = merged["__ubicacion_lookup"].where(
            merged["__ubicacion_lookup"].astype(str).str.strip().ne(""),
            merged["__ubicacion_lookup_item"],
        )
        merged = merged.drop(columns=["__ubicacion_lookup_item"])
    merged[c_ubicacion] = merged["__ubicacion_lookup"].where(
        merged["__ubicacion_lookup"].astype(str).str.strip().ne(""),
        merged[c_ubicacion].fillna(""),
    )
    merged = merged.drop(columns=["__ubicacion_lookup"])

    if overwrite:
        out_path = xlsx_path
    else:
        out_path = xlsx_path.with_name(f"{xlsx_path.stem}_con_ubicacion{xlsx_path.suffix}")

    merged.to_excel(out_path, index=False)

    missing = int((merged[c_ubicacion].fillna("").astype(str).str.strip() == "").sum())
    print(f"OK - {xlsx_path.name} -> {out_path.name} | sin match (ubicacion vacía): {missing}")
    return out_path


def _load_processed() -> set[str]:
    processed_state = _processed_state_path()
    if not processed_state.exists():
        return set()
    try:
        data = json.loads(processed_state.read_text(encoding="utf-8"))
        return set(data.get("files", []))
    except Exception:
        return set()


def _save_processed(files: set[str]) -> None:
    processed_state = _processed_state_path()
    processed_state.write_text(json.dumps({"files": sorted(files)}, ensure_ascii=False, indent=2), encoding="utf-8")


def _iter_input_excels() -> list[Path]:
    files = sorted(BASE_DIR.glob("*.xlsx"))
    out: list[Path] = []
    for p in files:
        if p.name.lower() == "elementos.xlsx":
            continue
        if p.name.lower().endswith("_con_ubicacion.xlsx"):
            continue
        if p.name.startswith("~$"):  # excel temp
            continue
        out.append(p)
    return out


def run_once(overwrite: bool, only: str | None) -> None:
    lookup = load_elementos_lookup()

    targets = _iter_input_excels()
    if only:
        only_path = (BASE_DIR / only).resolve()
        targets = [p for p in targets if p.resolve() == only_path]
        if not targets:
            raise SystemExit(f"No encontré el archivo {only_path.name} para procesar en {BASE_DIR}")

    if not targets:
        print("No hay archivos .xlsx para completar (además de elementos.xlsx).")
        return

    for p in targets:
        completar_archivo(p, lookup=lookup, overwrite=overwrite)


def watch(interval_s: float, overwrite: bool) -> None:
    lookup = load_elementos_lookup()
    processed = _load_processed()
    print(f"Escuchando {BASE_DIR} cada {interval_s:.1f}s. Subí un .xlsx y lo completo automáticamente.")

    try:
        while True:
            for p in _iter_input_excels():
                key = f"{p.name}|{p.stat().st_size}|{int(p.stat().st_mtime)}"
                if key in processed:
                    continue
                # evitar agarrar el archivo mientras Excel lo está escribiendo
                time.sleep(0.4)
                try:
                    completar_archivo(p, lookup=lookup, overwrite=overwrite)
                    processed.add(key)
                    _save_processed(processed)
                except PermissionError:
                    # todavía abierto por Excel, reintenta en la próxima vuelta
                    continue
            time.sleep(interval_s)
    except KeyboardInterrupt:
        print("Watch detenido.")


def main() -> None:
    global BASE_DIR
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--dir",
        type=str,
        default=str(BASE_DIR),
        help="Directorio donde están elementos.xlsx y los excels a completar (útil para Docker)",
    )
    ap.add_argument("--watch", action="store_true", help="Se queda escuchando la carpeta y procesa archivos nuevos")
    ap.add_argument("--interval-s", type=float, default=3.0, help="Intervalo de escaneo en segundos (watch)")
    ap.add_argument("--overwrite", action="store_true", help="Sobrescribe el Excel original (por defecto crea *_con_ubicacion.xlsx)")
    ap.add_argument("--only", type=str, default=None, help="Procesa solo este archivo (nombre dentro de la carpeta)")
    args = ap.parse_args()

    BASE_DIR = Path(args.dir).resolve()

    if args.watch:
        watch(interval_s=args.interval_s, overwrite=args.overwrite)
    else:
        run_once(overwrite=args.overwrite, only=args.only)


if __name__ == "__main__":
    main()


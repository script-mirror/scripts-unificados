import datetime
import os
from typing import List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from inewave.newave import Clast

from middle.utils import get_auth_header, sanitize_string, setup_logger

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), ".env"))


API_BASE_URL = "https://tradingenergiarz.com/api/v2/decks"
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%d/%m %H:%M"
UTE_PREFIX = "UTE"

CVU_COLUMN_MAPPING = {
    "cd_usina": "codigo_usina",
    "empreendimento": "nome_usina",
    "vl_cvu": "valor",
    "sigla_parcela": "nome_usina",
    "termino_suprimento": "data_fim",
    "inicio_suprimento": "data_inicio",
}

logger = setup_logger()


def get_usinas():
    res = requests.get(
        "https://tradingenergiarz.com/api/v2/decks/cvu/usinas",
        headers=get_auth_header(),
    )
    if res.status_code != 200:
        raise Exception(f"Erro ao obter usinas: {res.status_code} - {res.text}")
    return pd.DataFrame(res.json())


def _normalize_plant_name(text: str) -> str:
    """Normalize plant name by removing UTE prefix and sanitizing."""
    normalized = text.upper().replace(UTE_PREFIX, "")
    normalized = sanitize_string(normalized)
    return normalized.upper()


def _parse_dates(cvu_data: pd.DataFrame) -> pd.DataFrame:
    """Parse date columns in CVU data."""
    try:
        cvu_data["data_inicio"] = pd.to_datetime(
            cvu_data["data_inicio"], format=DATE_FORMAT
        )
        cvu_data["data_fim"] = pd.to_datetime(
            cvu_data["data_fim"], format=DATE_FORMAT
        )
    except Exception as e:
        logger.warning(f"Failed to parse dates: {e}")
    return cvu_data


def process_cvu_conjuntural_data(cvu_raw_data: pd.DataFrame) -> pd.DataFrame:
    """Process raw CVU data and return formatted DataFrame."""
    cvu_data = cvu_raw_data.drop_duplicates(["cd_usina"], keep="last")
    cvu_data = cvu_data.dropna(how="all", axis=1)

    cvu_data = cvu_data.rename(columns=CVU_COLUMN_MAPPING).copy()

    cvu_data["comentarios"] = ""
    cvu_data["custo"] = cvu_data["valor"]

    cvu_data = _parse_dates(cvu_data)
    cvu_data["nome_usina"] = cvu_data["nome_usina"].apply(
        _normalize_plant_name
    )

    return cvu_data


def filter_cvu_valid_plants(
    cvu_data: pd.DataFrame, valid_plants: List[str]
) -> pd.DataFrame:
    """Filter CVU data to include only valid plants."""
    return cvu_data[cvu_data["codigo_usina"].isin(valid_plants)]


def _should_remove_conjuntural_modification(
    modificacoes_temp: pd.DataFrame,
    usinas_a_alterar: List[str],
    tipo_cvu: str,
) -> pd.Series:
    """Determine which conjuntural modifications should be removed."""
    if tipo_cvu == "merchant":
        return ~(modificacoes_temp["comentarios"].str.contains(
            "merchant", case=False, na=False
        ))
    else:
        return ~(
            modificacoes_temp["codigo_usina"].isin(usinas_a_alterar)
            & ~modificacoes_temp["data_fim"].isna()
            & ~modificacoes_temp["comentarios"].str.contains(
                "merchant", case=False, na=False
            )
        )


def update_conjuntural_modifications_with_cvu(
    modificacoes: pd.DataFrame,
    cvu_data: pd.DataFrame,
    tipo_cvu: str,
) -> pd.DataFrame:
    """Update conjuntural modifications by removing
    existing entries and adding new CVU data."""
    usinas_a_alterar = cvu_data["codigo_usina"].unique().tolist()

    cvu_datas = cvu_data[["codigo_usina", "data_fim"]].rename(
        columns={"data_fim": "data_fim_cvu"}
    )
    modificacoes_temp = modificacoes.merge(
        cvu_datas, on="codigo_usina", how="left"
    )

    keep_mask = _should_remove_conjuntural_modification(
        modificacoes_temp, usinas_a_alterar, tipo_cvu
    )
    modificacoes_filtradas = modificacoes[keep_mask]
    cvu_data_filtered = cvu_data[modificacoes.columns]

    return pd.concat(
        [modificacoes_filtradas, cvu_data_filtered], ignore_index=True
    )


def _create_change_comment_conjuntural(
    timestamp: str,
    old_cost: Optional[float],
    new_cost: float,
    tipo_cvu: str,
) -> str:
    """Create a comment string for cost changes."""
    old_cost_str = str(old_cost) if old_cost is not None else "None"
    return f"{timestamp}\t{old_cost_str:>8} -> {str(new_cost):>8} {tipo_cvu}"


def log_conjuntural_plant_changes(
    usinas_a_alterar: List[str],
    modificacoes_original: pd.DataFrame,
    cvu_data_filtered: pd.DataFrame,
    tipo_cvu: str,
) -> pd.DataFrame:
    """Generate DataFrame with changes made to plants in conjuntural block."""
    timestamp = datetime.datetime.now().strftime(TIMESTAMP_FORMAT)
    alteracoes = []
    tipo_cvu_upper = tipo_cvu.upper()

    for codigo_usina in usinas_a_alterar:
        original_mask = (
            (modificacoes_original["codigo_usina"] == codigo_usina) &
            (~modificacoes_original["data_fim"].isna())
        )
        original_data = modificacoes_original[original_mask]

        new_data = cvu_data_filtered[
            cvu_data_filtered["codigo_usina"] == codigo_usina
        ]

        old_cost = original_data.iloc[0]["custo"] if len(
            original_data
        ) > 0 else None

        for _, new_row in new_data.iterrows():
            comentario = _create_change_comment_conjuntural(
                timestamp, old_cost, new_row["custo"], tipo_cvu_upper
            )
            alteracoes.append(
                {
                    "codigo_usina": codigo_usina,
                    "data_inicio": new_row["data_inicio"],
                    "data_fim": new_row["data_fim"],
                    "custo": new_row["custo"],
                    "comentarios": comentario,
                }
            )

    return pd.DataFrame(alteracoes)


def _update_clast_conjuntural_modifications(
    clast: Clast, alteracoes_df: pd.DataFrame
) -> None:
    """Update CLAST conjuntural modifications with changes DataFrame."""
    if alteracoes_df.empty:
        return

    for _, alteracao in alteracoes_df.iterrows():
        mask = (
            (clast.modificacoes["codigo_usina"] == alteracao["codigo_usina"]) &
            (clast.modificacoes["data_inicio"] == alteracao["data_inicio"]) &
            (clast.modificacoes["data_fim"] == alteracao["data_fim"]) &
            (abs(clast.modificacoes["custo"] - alteracao["custo"]) < 0.01) &
            (clast.modificacoes["comentarios"] == "")
        )

        clast.modificacoes.loc[mask, "comentarios"] = alteracao["comentarios"]


def _clean_conjuntural_duplicate_comments(
    clast: Clast, modificacoes_original: pd.DataFrame, tipo_cvu: str
) -> None:
    """Clean comments for duplicate entries in conjuntural block,
    keeping only comments for records that were actually updated."""

    grupos_duplicados = clast.modificacoes.groupby("codigo_usina").size() > 1
    usinas_duplicadas = grupos_duplicados[grupos_duplicados].index
    if len(usinas_duplicadas) > 0:
        for codigo_usina in usinas_duplicadas:

            mask_atual = clast.modificacoes["codigo_usina"] == codigo_usina
            registros_atuais = clast.modificacoes[mask_atual]

            if tipo_cvu == "merchant":
                mask_original = (
                    (modificacoes_original["codigo_usina"] == codigo_usina) &
                    modificacoes_original["comentarios"].str.contains(
                        "merchant", case=False, na=False
                    )
                )
            else:
                mask_original = (
                    (modificacoes_original["codigo_usina"] == codigo_usina) &
                    ~modificacoes_original["comentarios"].str.contains(
                        "merchant", case=False, na=False
                    )
                )

            registros_originais = modificacoes_original[mask_original]

            for idx, registro_atual in registros_atuais.iterrows():
                is_merchant_record = "merchant" in str(
                    registro_atual["comentarios"]
                ).lower()
                should_process = (tipo_cvu == "merchant") == is_merchant_record

                if not should_process:
                    continue

                for _, registro_original in registros_originais.iterrows():
                    if (
                        registro_atual["data_inicio"]
                        == registro_original["data_inicio"]
                        and registro_atual["data_fim"]
                        == registro_original["data_fim"]
                        and abs(registro_atual["custo"] - registro_original["custo"])
                        < 0.01
                    ):
                        clast.modificacoes.loc[idx, "comentarios"] = ""
                        break


def process_clast_conjuntural_update(
    cvu_data: pd.DataFrame,
    tipo_cvu: str,
    clast: Clast,
) -> None:
    """Process CLAST conjuntural block (modificações) update with CVU data."""
    usinas_validas = clast.usinas["codigo_usina"].unique().tolist()

    if tipo_cvu == "merchant":
        modificacoes_original = clast.modificacoes[
            clast.modificacoes["comentarios"].str.contains(
                "merchant", case=False, na=False
            )
        ].copy()
    else:
        modificacoes_original = clast.modificacoes[
            ~clast.modificacoes["comentarios"].str.contains(
                "merchant", case=False, na=False
            )
        ].copy()

    cvu_data_filtrado = filter_cvu_valid_plants(
        cvu_data, usinas_validas
    )

    clast.modificacoes = update_conjuntural_modifications_with_cvu(
        clast.modificacoes, cvu_data_filtrado, tipo_cvu
    )

    usinas_a_alterar = cvu_data_filtrado["codigo_usina"].unique().tolist()
    cvu_data_filtered = cvu_data_filtrado[clast.modificacoes.columns]
    alteracoes_df = log_conjuntural_plant_changes(
        usinas_a_alterar, modificacoes_original, cvu_data_filtered, tipo_cvu
    )

    _update_clast_conjuntural_modifications(clast, alteracoes_df)

    _clean_conjuntural_duplicate_comments(
        clast, modificacoes_original, tipo_cvu
    )


def process_cvu_estrutural_data(cvu_data: pd.DataFrame) -> pd.DataFrame:
    """Process raw CVU estrutural data and return formatted DataFrame."""
    if cvu_data.empty:
        return pd.DataFrame()

    cvu_data.drop_duplicates(
        ["cd_usina", "ano_horizonte"], keep="last", inplace=True
    )
    cvu_data = cvu_data.dropna(how="all", axis=1)
    cvu_data = cvu_data.rename(columns=CVU_COLUMN_MAPPING).copy()

    cvu_data["comentarios"] = ""

    cvu_data = _parse_dates(cvu_data)
    cvu_data["nome_usina"] = cvu_data["nome_usina"].apply(
        _normalize_plant_name
    )
    cvu_data["indice_ano_estudo"] = (
        cvu_data["ano_horizonte"] - cvu_data["ano_horizonte"].min() + 1
    )

    return cvu_data


def _create_change_comment_estrutural(
    timestamp: str, changes: list[tuple[int, float, float]]
) -> str:
    corpo = " | ".join(
        f"{indice}:\t{new-old:>7.2f}\t" for indice, old, new in changes
    )
    return f"{timestamp}\t{corpo}"


# comentario com diff ao inves de comparação: validar com gilseu
def _create_change_comment_conjuntural(
    timestamp: str,
    old_cost: Optional[float],
    new_cost: float,
    tipo_cvu: str,
) -> str:
    """Create a comment string for cost changes."""
    diff = new_cost - old_cost if old_cost is not None else 0.00
    return f"{timestamp} {diff:>5.2} {tipo_cvu}"


def update_estrutural_modifications_with_cvu(
    usinas: pd.DataFrame, cvu_data: pd.DataFrame
) -> pd.DataFrame:
    timestamp = pd.Timestamp.now().strftime(TIMESTAMP_FORMAT)

    # Identificar usinas que sofrerão alteração
    mask = usinas["codigo_usina"].isin(cvu_data["codigo_usina"])
    usinas_originais = usinas[mask].copy()
    usinas = usinas[~mask]

    # Merge por chave composta
    merged = pd.merge(
        usinas_originais[["codigo_usina", "indice_ano_estudo", "valor"]],
        cvu_data[["codigo_usina", "indice_ano_estudo", "valor"]],
        on=["codigo_usina", "indice_ano_estudo"],
        how="inner",
        suffixes=("_old", "_new"),
    )

    # Agrupar por codigo_usina e criar logs
    comentarios_por_usina = (
        merged.groupby("codigo_usina")
        .apply(
            lambda group: _create_change_comment_estrutural(
                timestamp,
                list(
                    zip(
                        group["indice_ano_estudo"],
                        group["valor_old"],
                        group["valor_new"],
                    )
                ),
            ),
            include_groups=False,
        )
        .to_dict()
    )

    # Mapear comentários em cvu_data
    cvu_data = cvu_data.copy()
    cvu_data["comentarios"] = cvu_data["codigo_usina"].map(comentarios_por_usina)

    # Concatenar resultado final
    return pd.concat([usinas, cvu_data], ignore_index=True)


def process_clast_estrutural_update(
    cvu_data: pd.DataFrame, clast: Clast
) -> None:
    """Process CLAST estrutural block (modificações) update with CVU data."""
    usinas_validas = clast.usinas["codigo_usina"].unique().tolist()
    cvu_data_filtrado = filter_cvu_valid_plants(
        cvu_data, usinas_validas
    )
    cvu_data_filtrado = cvu_data_filtrado[clast.usinas.columns]

    clast.usinas = update_estrutural_modifications_with_cvu(
        clast.usinas, cvu_data_filtrado
    )


def atualizar_cvu_clast_conjuntural(
    paths_clast: List[str],
    cvu_data: pd.DataFrame,
) -> None:
    tipos_cvu = cvu_data["fonte"].str.replace(
        "ccee_", "", case=False
    ).unique().tolist()
    cvu_data = process_cvu_conjuntural_data(cvu_data)
    paths_modified = []

    for path_clast in paths_clast:
        clast = Clast.read(path_clast)

        for tipo_cvu in tipos_cvu:

            # Filter CVU data for current type
            cvu_data_tipo = cvu_data[
                cvu_data["fonte"].str.replace("ccee_", "", case=False) == tipo_cvu
            ]
            process_clast_conjuntural_update(
                cvu_data_tipo, tipo_cvu, clast
            )

        clast.write(path_clast)
        paths_modified.append(path_clast)

        clast_verify = Clast.read(path_clast)
        clast_verify.write(path_clast)
    return paths_modified


def atualizar_cvu_clast_estrutural(
    paths_clast: List[str],
    cvu_data: pd.DataFrame,
) -> List[str]:
    cvu_data = process_cvu_estrutural_data(cvu_data)
    paths_modified = []

    for path_clast in paths_clast:
        clast = Clast.read(path_clast)

        process_clast_estrutural_update(cvu_data, clast)

        clast.write(path_clast)
        paths_modified.append(path_clast)

        clast_verify = Clast.read(path_clast)
        clast_verify.write(path_clast)

    return paths_modified


if __name__ == "__main__":
    pass

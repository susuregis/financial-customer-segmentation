import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from src.config.config import Config
from src.utils.database import (
    AccountDim,
    CategoryDim,
    CurrencyDim,
    CustomerDim,
    DatabaseManager,
    DateDim,
    ExchangeRateFact,
    TransactionFact,
)
from src.utils.logger import LoggerConfig


logger = LoggerConfig.setup_logging(__name__)


class GoldLayerAggregation:
    CURRENCY_REFERENCE = {
        "BRL": ("Real Brasileiro", "Brasil"),
        "USD": ("Dolar Americano", "Estados Unidos"),
        "EUR": ("Euro", "Uniao Europeia"),
        "GBP": ("Libra Esterlina", "Reino Unido"),
        "JPY": ("Iene", "Japao"),
        "CAD": ("Dolar Canadense", "Canada"),
        "ARS": ("Peso Argentino", "Argentina"),
    }

    def __init__(self, db_manager: DatabaseManager):
        self.gold_path = Path(Config.DATA_GOLD_PATH)
        self.db_manager = db_manager
        self.gold_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Camada gold pronta em {self.gold_path}")

    def create_dimensions(self, clientes_df, categorias_df, taxa_cambio_df) -> Dict[str, Dict[str, int]]:
        session = self.db_manager.get_session()
        mappings = {"customer": {}, "category": {}, "currency": {}, "account": {}}
        clientes = self._to_pandas(clientes_df)
        categorias = self._to_pandas(categorias_df)
        taxas = self._to_pandas(taxa_cambio_df)

        try:
            moedas = set(taxas.get("currency", pd.Series(dtype=str)).dropna().astype(str).str.upper().tolist())
            moedas.add("BRL")
            if "pais_cliente" in clientes.columns:
                for pais in clientes["pais_cliente"]:
                    moeda = self._currency_from_country(self._normalize_country(pais))
                    moedas.add(moeda)

            for moeda in sorted(moedas):
                nome_moeda, pais_moeda = self.CURRENCY_REFERENCE.get(
                    moeda,
                    (moeda, "Nao informado"),
                )
                existente = session.query(CurrencyDim).filter_by(currency_code=moeda).first()
                if existente is None:
                    existente = CurrencyDim(
                        currency_code=moeda,
                        currency_name=nome_moeda,
                        country=pais_moeda,
                    )
                    session.add(existente)
                    session.flush()
                mappings["currency"][moeda] = existente.currency_id

            for _, linha in clientes.iterrows():
                customer_code = str(linha["id_cliente"])
                customer_name = self._safe_str(linha.get("nome_cliente"))
                country = self._normalize_country(linha.get("pais_cliente"))
                existente = session.query(CustomerDim).filter_by(customer_code=customer_code).first()
                if existente is None:
                    existente = CustomerDim(
                        customer_code=customer_code,
                        customer_name=customer_name,
                        country=country,
                    )
                    session.add(existente)
                    session.flush()
                else:
                    existente.customer_name = customer_name
                    existente.country = country
                mappings["customer"][customer_code] = existente.customer_id

            for _, linha in categorias.iterrows():
                category_code = self._safe_str(linha.get("codigo_categoria"))
                category_name = self._safe_str(linha.get("nome_categoria"))
                description = self._safe_str(linha.get("tipo_categoria"))
                existente = session.query(CategoryDim).filter_by(category_code=category_code).first()
                if existente is None:
                    existente = CategoryDim(
                        category_code=category_code,
                        category_name=category_name,
                        description=description,
                    )
                    session.add(existente)
                    session.flush()
                else:
                    existente.category_name = category_name
                    existente.description = description
                mappings["category"][category_code] = existente.category_id

            for account_type in ["PAGAR", "RECEBER"]:
                for category_code, category_id in mappings["category"].items():
                    account_code = f"{account_type}|{category_code}"
                    existente = session.query(AccountDim).filter_by(account_code=account_code).first()
                    if existente is None:
                        existente = AccountDim(
                            account_code=account_code,
                            account_type=account_type,
                            category_id=category_id,
                        )
                        session.add(existente)
                        session.flush()
                    mappings["account"][account_code] = existente.account_id

            session.commit()
            logger.info("Dimensoes atualizadas com sucesso")
            return mappings
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_date_dimension(self, start_date_str: str, end_date_str: str):
        session = self.db_manager.get_session()
        try:
            inicio = pd.to_datetime(start_date_str).date()
            fim = pd.to_datetime(end_date_str).date()
            datas_existentes = {
                registro.date
                for registro in session.query(DateDim).filter(DateDim.date >= inicio, DateDim.date <= fim).all()
            }
            cursor = inicio
            novos_registros = []
            while cursor <= fim:
                if cursor not in datas_existentes:
                    novos_registros.append(
                        DateDim(
                            date=cursor,
                            year=cursor.year,
                            month=cursor.month,
                            day=cursor.day,
                            quarter=((cursor.month - 1) // 3) + 1,
                            week=cursor.isocalendar()[1],
                            day_of_week=cursor.strftime("%A"),
                            is_weekend=1 if cursor.weekday() >= 5 else 0,
                        )
                    )
                cursor += timedelta(days=1)
            if novos_registros:
                session.add_all(novos_registros)
                session.commit()
            logger.info("Dimensao de datas atualizada")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def load_transactions_to_fact_table(
        self,
        contas_pagar_df,
        contas_receber_df,
        mappings: Dict[str, Dict[str, int]],
        exchange_rates_df,
        clientes_df,
        categorias_df,
    ) -> pd.DataFrame:
        session = self.db_manager.get_session()
        pagar = self._to_pandas(contas_pagar_df)
        receber = self._to_pandas(contas_receber_df)
        taxas = self._to_pandas(exchange_rates_df)
        clientes = self._to_pandas(clientes_df)
        categorias = self._to_pandas(categorias_df)

        clientes_lookup = self._build_customer_lookup(clientes)
        categorias_lookup = self._build_category_lookup(categorias)
        taxas_lookup = self._build_rate_lookup(taxas)

        try:
            session.query(TransactionFact).delete()
            session.query(ExchangeRateFact).delete()
            session.commit()

            self._load_exchange_rates(session, taxas, mappings["currency"])
            date_mapping = self._create_date_mapping(session)

            registros = []
            for _, linha in pagar.iterrows():
                registro = self._build_transaction_record(
                    linha=linha,
                    transaction_type="PAGAR",
                    customer_data=None,
                    mappings=mappings,
                    date_mapping=date_mapping,
                    taxas_lookup=taxas_lookup,
                    categorias_lookup=categorias_lookup,
                )
                if registro is not None:
                    registros.append(registro)

            for _, linha in receber.iterrows():
                customer_data = clientes_lookup.get(self._safe_str(linha.get("id_cliente")))
                registro = self._build_transaction_record(
                    linha=linha,
                    transaction_type="RECEBER",
                    customer_data=customer_data,
                    mappings=mappings,
                    date_mapping=date_mapping,
                    taxas_lookup=taxas_lookup,
                    categorias_lookup=categorias_lookup,
                )
                if registro is not None:
                    registros.append(registro)

            fatos = []
            for registro in registros:
                fatos.append(
                    TransactionFact(
                        customer_id=registro["customer_dim_id"],
                        account_id=registro["account_id"],
                        currency_id=registro["currency_id"],
                        date_id=registro["date_id"],
                        amount_original=registro["valor_original"],
                        amount_brl=registro["valor_brl"],
                        exchange_rate=registro["taxa_cambio"],
                        transaction_date=datetime.combine(registro["data_referencia"], datetime.min.time()),
                    )
                )
            if fatos:
                session.add_all(fatos)
                session.commit()

            transacoes = pd.DataFrame(registros)
            if not transacoes.empty:
                caminho = self.gold_path / "transactions_enriched.parquet"
                transacoes.to_parquet(caminho, index=False)
                logger.info(f"Transacoes enriquecidas salvas em {caminho}")
            return transacoes
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_aggregated_views(self, transacoes_df: pd.DataFrame):
        transacoes = self._to_pandas(transacoes_df)
        if transacoes.empty:
            raise ValueError("Nao ha transacoes enriquecidas para consolidar na gold")

        transacoes["data_referencia"] = pd.to_datetime(transacoes["data_referencia"])

        customer_base = transacoes[
            (transacoes["tipo_conta"] == "receber") & transacoes["id_cliente"].notna()
        ].copy()
        customer_summary = (
            customer_base.groupby(
                ["id_cliente", "nome_cliente", "pais_cliente", "currency"],
                as_index=False,
            )
            .agg(
                total_receita_original=("valor_original", "sum"),
                total_receita_brl=("valor_brl", "sum"),
                ticket_medio_brl=("valor_brl", "mean"),
                quantidade_transacoes=("id_conta", "count"),
                desvio_padrao_brl=("valor_brl", "std"),
                percentual_atraso=("status_conta", self._percentual_atraso),
                percentual_pago=("status_conta", self._percentual_pago),
                primeira_transacao=("data_referencia", "min"),
                ultima_transacao=("data_referencia", "max"),
            )
        )
        customer_summary["desvio_padrao_brl"] = customer_summary["desvio_padrao_brl"].fillna(0.0)
        customer_summary["dias_ativos"] = (
            customer_summary["ultima_transacao"] - customer_summary["primeira_transacao"]
        ).dt.days.fillna(0)
        self._salvar_gold(customer_summary, "customer_summary")

        category_summary = (
            transacoes.groupby(["tipo_conta", "codigo_categoria", "nome_categoria"], as_index=False)
            .agg(
                total_original=("valor_original", "sum"),
                total_brl=("valor_brl", "sum"),
                quantidade_transacoes=("id_conta", "count"),
            )
        )
        self._salvar_gold(category_summary, "category_summary")

        monthly_summary = transacoes.copy()
        monthly_summary["ano_mes"] = monthly_summary["data_referencia"].dt.strftime("%Y-%m")
        monthly_summary = (
            monthly_summary.groupby(["ano_mes", "tipo_conta"], as_index=False)
            .agg(
                total_original=("valor_original", "sum"),
                total_brl=("valor_brl", "sum"),
                quantidade_transacoes=("id_conta", "count"),
            )
        )
        self._salvar_gold(monthly_summary, "monthly_summary")
        logger.info("Camada gold consolidada com sucesso")

    def update_customer_clusters(self, segmented_df: pd.DataFrame):
        if segmented_df.empty:
            return
        session = self.db_manager.get_session()
        try:
            for _, linha in segmented_df.iterrows():
                codigo = self._safe_str(linha.get("id_cliente"))
                cluster = int(linha.get("cluster"))
                customer = session.query(CustomerDim).filter_by(customer_code=codigo).first()
                if customer is not None:
                    customer.ml_cluster = cluster
            session.commit()
            logger.info("Clusters de clientes atualizados no banco")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _load_exchange_rates(self, session, taxas: pd.DataFrame, currency_mapping: Dict[str, int]):
        taxas = taxas.copy()
        if taxas.empty:
            return
        taxas["date"] = pd.to_datetime(taxas["date"]).dt.date
        taxas = taxas.drop_duplicates(["currency", "date"])
        registros = []
        for _, linha in taxas.iterrows():
            moeda = self._safe_str(linha.get("currency")).upper()
            currency_id = currency_mapping.get(moeda)
            if currency_id is None:
                continue
            registros.append(
                ExchangeRateFact(
                    currency_id=currency_id,
                    date=linha["date"],
                    buy_rate=float(linha["buy_rate"]),
                    sell_rate=float(linha["sell_rate"]),
                    average_rate=float(linha["average_rate"]),
                )
            )
        if registros:
            session.add_all(registros)
            session.commit()

    def _build_transaction_record(
        self,
        linha: pd.Series,
        transaction_type: str,
        customer_data: Optional[Dict],
        mappings: Dict[str, Dict[str, int]],
        date_mapping: Dict[date, int],
        taxas_lookup: Dict[str, list],
        categorias_lookup: Dict[str, str],
    ) -> Optional[Dict]:
        codigo_categoria = self._safe_str(linha.get("codigo_categoria"))
        account_code = f"{transaction_type}|{codigo_categoria}"
        account_id = mappings["account"].get(account_code)
        if account_id is None:
            return None

        data_referencia = self._resolve_transaction_date(linha)
        if data_referencia is None:
            return None
        date_id = date_mapping.get(data_referencia)
        if date_id is None:
            return None

        if transaction_type == "RECEBER" and customer_data is not None:
            pais_cliente = customer_data["pais_cliente"]
            nome_cliente = customer_data["nome_cliente"]
            id_cliente = customer_data["id_cliente"]
            customer_dim_id = mappings["customer"].get(id_cliente)
            moeda = self._currency_from_country(pais_cliente)
        else:
            pais_cliente = "BRASIL"
            nome_cliente = None
            id_cliente = None
            customer_dim_id = None
            moeda = "BRL"

        if moeda != "BRL" and moeda not in taxas_lookup:
            logger.warning(f"Taxa de cambio indisponivel para {moeda}. Transacao sera tratada como BRL")
            moeda = "BRL"

        currency_id = mappings["currency"].get(moeda, mappings["currency"]["BRL"])
        valor_original = float(linha.get("valor_conta", 0) or 0)
        taxa_cambio = self._resolve_rate(moeda, data_referencia, taxas_lookup)
        valor_brl = round(valor_original * taxa_cambio, 2)
        status_conta = self._safe_str(linha.get("status_conta"))

        return {
            "id_conta": self._safe_str(linha.get("id_conta")),
            "tipo_conta": transaction_type.lower(),
            "id_cliente": id_cliente,
            "nome_cliente": nome_cliente,
            "pais_cliente": pais_cliente,
            "codigo_categoria": codigo_categoria,
            "nome_categoria": categorias_lookup.get(codigo_categoria),
            "status_conta": status_conta,
            "currency": moeda,
            "taxa_cambio": taxa_cambio,
            "valor_original": round(valor_original, 2),
            "valor_brl": valor_brl,
            "data_referencia": data_referencia,
            "customer_dim_id": customer_dim_id,
            "account_id": account_id,
            "currency_id": currency_id,
            "date_id": date_id,
        }

    def _build_customer_lookup(self, clientes: pd.DataFrame) -> Dict[str, Dict]:
        lookup = {}
        for _, linha in clientes.iterrows():
            codigo = self._safe_str(linha.get("id_cliente"))
            lookup[codigo] = {
                "id_cliente": codigo,
                "nome_cliente": self._safe_str(linha.get("nome_cliente")),
                "pais_cliente": self._normalize_country(linha.get("pais_cliente")),
            }
        return lookup

    def _build_category_lookup(self, categorias: pd.DataFrame) -> Dict[str, str]:
        lookup = {}
        for _, linha in categorias.iterrows():
            codigo = self._safe_str(linha.get("codigo_categoria"))
            lookup[codigo] = self._safe_str(linha.get("nome_categoria"))
        return lookup

    def _build_rate_lookup(self, taxas: pd.DataFrame) -> Dict[str, list]:
        if taxas.empty:
            return {}
        taxas = taxas.copy()
        taxas["date"] = pd.to_datetime(taxas["date"]).dt.date
        taxas["currency"] = taxas["currency"].astype(str).str.upper()
        lookup = {}
        for moeda, grupo in taxas.groupby("currency"):
            lookup[moeda] = sorted(
                [(linha["date"], float(linha["average_rate"])) for _, linha in grupo.iterrows()],
                key=lambda item: item[0],
            )
        return lookup

    def _resolve_rate(self, currency: str, reference_date: date, taxas_lookup: Dict[str, list]) -> float:
        if currency == "BRL":
            return 1.0
        historico = taxas_lookup.get(currency, [])
        if not historico:
            raise ValueError(f"Taxa de cambio nao encontrada para a moeda {currency}")
        taxa_selecionada = historico[0][1]
        for data_taxa, taxa in historico:
            if data_taxa <= reference_date:
                taxa_selecionada = taxa
            else:
                break
        return round(taxa_selecionada, 4)

    def _resolve_transaction_date(self, linha: pd.Series) -> Optional[date]:
        for coluna in ["data_pagamento", "data_vencimento"]:
            valor = linha.get(coluna)
            if pd.notna(valor):
                return pd.to_datetime(valor).date()
        return None

    def _create_date_mapping(self, session) -> Dict[date, int]:
        return {registro.date: registro.date_id for registro in session.query(DateDim).all()}

    def _currency_from_country(self, country: str) -> str:
        mapa = {
            "BRASIL": "BRL",
            "ESTADOS UNIDOS": "USD",
            "PORTUGAL": "EUR",
            "FRANCA": "EUR",
            "REINO UNIDO": "GBP",
            "CANADA": "CAD",
            "ARGENTINA": "ARS",
        }
        return mapa.get(country, "BRL")

    def _normalize_country(self, value) -> str:
        texto = self._safe_str(value).upper()
        texto = (
            texto.replace("Ã", "A")
            .replace("Á", "A")
            .replace("À", "A")
            .replace("Â", "A")
            .replace("É", "E")
            .replace("Ê", "E")
            .replace("Í", "I")
            .replace("Ó", "O")
            .replace("Ô", "O")
            .replace("Õ", "O")
            .replace("Ú", "U")
            .replace("Ç", "C")
        )
        texto = re.sub(r"[^A-Z ]", " ", texto)
        texto = re.sub(r"\s+", " ", texto).strip()

        if texto in {"USA", "EUA"} or "ESTADOS UNIDOS" in texto or "AMERICA" in texto:
            return "ESTADOS UNIDOS"
        if texto in {"UK"} or "REINO UNIDO" in texto or "INGLATERRA" in texto:
            return "REINO UNIDO"
        if "BRASIL" in texto:
            return "BRASIL"
        if "PORTUGAL" in texto:
            return "PORTUGAL"
        if "FRAN" in texto:
            return "FRANCA"
        if "CANADA" in texto:
            return "CANADA"
        if "ARGENTINA" in texto:
            return "ARGENTINA"
        return texto or "NAO INFORMADO"

    def _salvar_gold(self, df: pd.DataFrame, nome_arquivo: str):
        caminho_parquet = self.gold_path / f"{nome_arquivo}.parquet"
        caminho_csv = self.gold_path / f"{nome_arquivo}.csv"
        df.to_parquet(caminho_parquet, index=False)
        df.to_csv(caminho_csv, index=False, encoding="utf-8")
        logger.info(f"Arquivo gold salvo em {caminho_parquet}")

    def _percentual_atraso(self, serie: pd.Series) -> float:
        return round(float((serie == "atrasado").mean() * 100), 2)

    def _percentual_pago(self, serie: pd.Series) -> float:
        return round(float((serie == "pago").mean() * 100), 2)

    def _safe_str(self, value) -> str:
        if pd.isna(value):
            return ""
        return str(value).strip()

    def _to_pandas(self, df) -> pd.DataFrame:
        if hasattr(df, "toPandas"):
            expressoes = []
            for nome_coluna, tipo_coluna in df.dtypes:
                if tipo_coluna.startswith("date") or tipo_coluna.startswith("timestamp"):
                    expressoes.append(f"CAST({nome_coluna} AS STRING) AS {nome_coluna}")
                else:
                    expressoes.append(nome_coluna)
            return df.selectExpr(*expressoes).toPandas()
        if isinstance(df, pd.DataFrame):
            return df.copy()
        return pd.DataFrame(df)

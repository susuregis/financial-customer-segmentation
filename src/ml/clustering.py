import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.metrics import calinski_harabasz_score, davies_bouldin_score, silhouette_score
from sklearn.preprocessing import StandardScaler

from src.config.config import Config
from src.utils.logger import LoggerConfig


logger = LoggerConfig.setup_logging(__name__)


class CustomerSegmentation:
    def __init__(self, n_clusters: int = 0):
        self.n_clusters = n_clusters
        self.output_path = Path(Config.DATA_GOLD_PATH) / "ml_results"
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.metrics: Dict[str, float] = {}
        self.cluster_metrics = pd.DataFrame()
        self.scaler = StandardScaler()

    def prepare_features(self, base_df: pd.DataFrame) -> pd.DataFrame:
        if base_df is None or base_df.empty:
            logger.warning("Nao ha base para preparar atributos de clustering")
            return pd.DataFrame()

        df = base_df.copy()
        if {"id_cliente", "total_receita_brl", "ticket_medio_brl", "quantidade_transacoes"}.issubset(df.columns):
            colunas = [
                "id_cliente",
                "nome_cliente",
                "pais_cliente",
                "currency",
                "total_receita_brl",
                "ticket_medio_brl",
                "quantidade_transacoes",
                "desvio_padrao_brl",
                "percentual_atraso",
                "percentual_pago",
                "dias_ativos",
            ]
            existentes = [coluna for coluna in colunas if coluna in df.columns]
            features = df[existentes].copy()
            for coluna in [
                "total_receita_brl",
                "ticket_medio_brl",
                "quantidade_transacoes",
                "desvio_padrao_brl",
                "percentual_atraso",
                "percentual_pago",
                "dias_ativos",
            ]:
                if coluna in features.columns:
                    features[coluna] = pd.to_numeric(features[coluna], errors="coerce").fillna(0.0)
            return features

        raise ValueError("A base de entrada do ML nao possui as colunas esperadas da camada gold")

    def select_best_k(self, features_df: pd.DataFrame) -> int:
        numericas = self._numeric_features(features_df)
        total_amostras = len(numericas)
        if total_amostras < 3:
            return 1

        max_clusters = min(Config.MAX_CLUSTERS, total_amostras - 1)
        min_clusters = min(Config.MIN_CLUSTERS, max_clusters)

        if self.n_clusters and self.n_clusters > 1:
            return min(self.n_clusters, max_clusters)

        dados_escalados = self.scaler.fit_transform(numericas)
        analise = []
        melhor_k = min_clusters
        melhor_silhueta = -1.0

        for k in range(min_clusters, max_clusters + 1):
            modelo = KMeans(n_clusters=k, random_state=Config.RANDOM_STATE, n_init=10)
            labels = modelo.fit_predict(dados_escalados)
            if len(set(labels)) < 2:
                continue
            silhueta = silhouette_score(dados_escalados, labels)
            analise.append({"k": k, "silhouette_score": silhueta, "inercia": modelo.inertia_})
            if silhueta > melhor_silhueta:
                melhor_silhueta = silhueta
                melhor_k = k

        analise_df = pd.DataFrame(analise)
        if not analise_df.empty:
            analise_df.to_csv(self.output_path / "analise_k.csv", index=False, encoding="utf-8")
            self._plot_k_analysis(analise_df)
        return melhor_k

    def fit(self, features_df: pd.DataFrame) -> Tuple[pd.Series, Dict[str, float]]:
        numericas = self._numeric_features(features_df)
        if numericas.empty:
            return pd.Series(dtype=int), {}

        k = self.select_best_k(features_df)
        if k <= 1:
            labels = pd.Series([0] * len(features_df), index=features_df.index)
            self.metrics = {
                "k_escolhido": 1,
                "silhouette_score": 0.0,
                "davies_bouldin_index": 0.0,
                "calinski_harabasz_index": 0.0,
                "inertia": 0.0,
                "n_amostras": len(features_df),
            }
            return labels, self.metrics

        dados_escalados = self.scaler.fit_transform(numericas)
        modelo = KMeans(n_clusters=k, random_state=Config.RANDOM_STATE, n_init=10)
        labels = pd.Series(modelo.fit_predict(dados_escalados), index=features_df.index)

        self.metrics = {
            "k_escolhido": k,
            "silhouette_score": float(silhouette_score(dados_escalados, labels)),
            "davies_bouldin_index": float(davies_bouldin_score(dados_escalados, labels)),
            "calinski_harabasz_index": float(calinski_harabasz_score(dados_escalados, labels)),
            "inertia": float(modelo.inertia_),
            "n_amostras": len(features_df),
        }

        cluster_metrics = (
            features_df.assign(cluster=labels)
            .groupby("cluster", as_index=False)
            .agg(
                quantidade_clientes=("id_cliente", "count"),
                total_receita_brl=("total_receita_brl", "sum"),
                ticket_medio_brl=("ticket_medio_brl", "mean"),
                quantidade_transacoes=("quantidade_transacoes", "mean"),
                percentual_atraso=("percentual_atraso", "mean"),
            )
        )
        self.cluster_metrics = cluster_metrics
        return labels, self.metrics

    def segment_customers(self, features_df: pd.DataFrame, labels: pd.Series) -> pd.DataFrame:
        segmentado = features_df.copy()
        segmentado["cluster"] = labels.values
        segmentado.to_csv(self.output_path / "customer_segments.csv", index=False, encoding="utf-8")
        segmentado.to_parquet(self.output_path / "customer_segments.parquet", index=False)
        return segmentado

    def visualize_clusters(self, segmented_df: pd.DataFrame):
        if segmented_df.empty or "cluster" not in segmented_df.columns:
            return

        plt.figure(figsize=(12, 8))
        sns.scatterplot(
            data=segmented_df,
            x="total_receita_brl",
            y="quantidade_transacoes",
            hue="cluster",
            palette="viridis",
        )
        plt.title("Segmentacao de clientes")
        plt.xlabel("Total recebido em BRL")
        plt.ylabel("Quantidade de transacoes")
        plt.tight_layout()
        plt.savefig(self.output_path / "clusters_plot.png")
        plt.close()

    def save_metrics_report(self):
        if not self.metrics:
            return

        metricas_df = pd.DataFrame(
            [
                {"metrica": chave, "valor": valor}
                for chave, valor in self.metrics.items()
            ]
        )
        metricas_df.to_csv(self.output_path / "metricas_clusterizacao.csv", index=False, encoding="utf-8")
        if not self.cluster_metrics.empty:
            self.cluster_metrics.to_csv(
                self.output_path / "metricas_por_cluster.csv",
                index=False,
                encoding="utf-8",
            )

    def run_segmentation(self, customer_summary_df: pd.DataFrame) -> pd.DataFrame:
        features = self.prepare_features(customer_summary_df)
        if features.empty:
            return pd.DataFrame()
        labels, _ = self.fit(features)
        segmentado = self.segment_customers(features, labels)
        self.save_metrics_report()
        self.visualize_clusters(segmentado)
        return segmentado

    def _numeric_features(self, features_df: pd.DataFrame) -> pd.DataFrame:
        colunas = [
            "total_receita_brl",
            "ticket_medio_brl",
            "quantidade_transacoes",
            "desvio_padrao_brl",
            "percentual_atraso",
            "percentual_pago",
            "dias_ativos",
        ]
        return features_df[colunas].fillna(0.0)

    def _plot_k_analysis(self, analise_df: pd.DataFrame):
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=analise_df, x="k", y="silhouette_score", marker="o")
        plt.title("Analise do numero ideal de clusters")
        plt.xlabel("Numero de clusters")
        plt.ylabel("Silhouette score")
        plt.tight_layout()
        plt.savefig(self.output_path / "analise_k.png")
        plt.close()

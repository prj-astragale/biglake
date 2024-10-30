# 1.0.dev304.1

import os, sys, re
from abc import ABC, abstractmethod
from logging import getLogger
from pathlib import Path
from string import Template
import uuid


import logging

# import asyncio

from urllib.parse import urlparse
from urllib.error import URLError

from app.loggers import logger_t  # Import

# logger_t = logging.getLogger()  # Declare
# logger_t.setLevel(logging.DEBUG)  # Declare

from rdflib import Graph, Namespace
from rdflib.plugins.stores import sparqlstore
from rdflib.plugins.sparql.processor import SPARQLResult

import pandas as pd

from typing import Optional, Mapping, Any

try:
    from functools import cache
except ImportError:
    from functools import lru_cache

    cache = lru_cache(maxsize=None)


class GraphStore(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        """"""

    # Select
    @abstractmethod
    def select_templated(
        self,
        query_filename: str,
        format: str = "dataframe",
        override_named_graph_uri: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """"""

    @abstractmethod
    def select_templated_parametrized(
        self,
        query_filename: str,
        format: str = "dataframe",
        enforce_parameters: Mapping[str, str] | None = None,
        override_named_graph_uri: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """"""

    # Update
    @abstractmethod
    def update_static(
        self,
        query_string: str,
    ) -> pd.DataFrame:
        """"""

    @abstractmethod
    def update_templated(self, query_filename: str, **kwargs) -> pd.DataFrame:
        """"""

    # Results
    def sparql_results_to_df(self, results: SPARQLResult) -> pd.DataFrame:
        """Export results from an rdflib SPARQL query into a `pandas.DataFrame`,
            using Python types. See https://github.com/RDFLib/rdflib/issues/1179.

        Args:
            results (SPARQLResult): _description_

        Returns:
            DataFrame: _description_
        """
        return pd.DataFrame(
            data=(
                [None if x is None else x.toPython() for x in row] for row in results
            ),
            columns=[str(x) for x in results.vars],
        )

    def sparql_results_to_json(self, results: SPARQLResult):
        """Export results from an rdflib SPARQL query into dictionary
        Warning: SLOW version, using converters from pd.dataframe via sparql_results_to_df
        TODO: build directly the json string

        Args:
            results (SPARQLResult): _description_

        Returns:
            DataFrame: _description_
        """
        return pd.DataFrame(
            data=(
                [None if x is None else x.toPython() for x in row] for row in results
            ),
            columns=[str(x) for x in results.vars],
        ).to_json(orient="records", force_ascii=False)

    def sparql_results_to_dict(self, results: SPARQLResult):
        """Export results from an rdflib SPARQL query into dictionary
        Warning: SLOW version, using converters from pd.dataframe via sparql_results_to_df
        TODO: build directly the json string

        Args:
            results (SPARQLResult): _description_

        Returns:
            DataFrame: _description_
        """
        return pd.DataFrame(
            data=(
                [None if x is None else x.toPython() for x in row] for row in results
            ),
            columns=[str(x) for x in results.vars],
        ).to_dict(orient="records")

    # Exception handling
    def raise_exceptions_query(func):
        def wrapper(self, *args, **kwargs):
            """

            kwargs['query_filename'],

            Returns:
                _type_: _description_
            """
            try:
                return func(self, *args, **kwargs)
            except FileNotFoundError as fnfe:
                logger_t.error(f"{type(fnfe)} {fnfe}")
                logger_t.error(
                    f"No SPARQL file found at path: {Path(self.config['datapip_sparql_select_path'], kwargs['query_filename'])}"
                )
                return fnfe
            except URLError as ue:
                logger_t.error(f"{type(ue)} {ue}")
                logger_t.error(
                    f"Error connecting RDF-graph database: query_endpoint={self.config_store['query_endpoint']}, update_endpoint={self.config_store['update_endpoint']}"
                )
                return ue
            except KeyError as ke:
                logger_t.error(f"{type(ke)} {ke}")
                logger_t.error(
                    f"JSON data input is not compliant with the Data Pipeline Schema for data substitution: schema={kwargs['query_filename'].split('.')[0]}"
                )
                # logger_t.error(
                #     f"Please, check missing value \n{Template(s).safe_substitute(kwargs)}"
                # )
                return ke
            except ValueError as ve:
                logger_t.error(f"{type(ve)} {ve}")
                logger_t.error(
                    f"Check connexion with the TripleStore: query_endpoint={self.config_store['query_endpoint']}, update_endpoint={self.config_store['update_endpoint']}"
                )
                return ve
            except Exception as e:
                logger_t.error(f"Unhandled error: {type(e)} {e}")
                return e

        return wrapper


class LocalTripleStore(GraphStore):
    def __init__(
        self,
        config: dict,
        config_namespaces: dict[str, str] | None = None,
        bootstrap_rdffiles: list[str | Path] | None = None,
    ):
        # Config
        self.config = config
        try:
            self.default_named_graph_uri = "".join(
                [
                    self.config["default_named_graph_root_uri"],
                    self.config["default_named_graph_name"],
                ]
            )
            self.default_triples_root_uri = self.config["default_triples_root_uri"]
        except:
            raise ValueError(
                f"GraphStore Class API configuration error, cannot define default named graph with provided config, config={self.config}"
            )

        # Graph
        self.g = Graph(bind_namespaces="none")

        if config_namespaces is not None:
            for name, uri in config_namespaces.items():
                self.g.bind(name, Namespace(uri))

        if bootstrap_rdffiles is not None:
            for rdffile in bootstrap_rdffiles:
                self.g.parse(rdffile)

    # Select
    @GraphStore.raise_exceptions_query
    def select_templated(
        self,
        query_filename: str,
        format: str = "dataframe",
        override_named_graph_uri: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        try:
            query_path = Path(self.config["datapip_sparql_select_path"], query_filename)
        except KeyError as ke1:
            logger_t.error(ke1)
            logger_t.error(
                f"Incomplete config for select queries, add  'datapip_sparql_select_path' to your config={self.config}"
            )
            return None

        with open(query_path, "r", encoding="utf-8") as file:
            if len(kwargs) == 0:
                query_string = file.read()
            else:
                s = file.read()
                query_string = Template(s).substitute(
                    kwargs
                )  # squery = s.format(**kwargs)

        logger_t.debug(f"Select Query:\n{query_string}")
        qr = self.g.query(query_string)

        match format:
            case "dataframe":
                return self.sparql_results_to_df(qr)
            case "json":
                return self.sparql_results_to_json(qr)
            case "dict":
                return self.sparql_results_to_dict(qr)
            case _:
                logger_t.warning(
                    f"Unrecoginzed format={format}, defaulting to 'dataframe' output"
                )
                return self.sparql_results_to_df(qr)

    @GraphStore.raise_exceptions_query
    def select_templated_parametrized(
        self,
        query_filename: str,
        format: str = "dataframe",
        enforce_parameters: Mapping[str, str] | None = None,
        named_graph_full_uri: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        logger_t.error(f"Not yet implemented for LocalTripleStore")

    # Update
    @GraphStore.raise_exceptions_query
    def update_static(self, query_string: str, **kwargs):
        self.g.update(query_string)

    @GraphStore.raise_exceptions_query
    def update_templated(self, query_filename: str, **kwargs) -> pd.DataFrame:
        try:
            query_path = Path(self.config["datapip_sparql_update_path"], query_filename)
        except KeyError as ke1:
            logger_t.error(ke1)
            logger_t.error(
                f"Incomplete config for select queries, add  'datapip_sparql_update_path' to your config={self.config}"
            )
            return None

        uris = {}
        with open(query_path, "r", encoding="utf-8") as file:
            s = file.read()

            uris_match = set(
                re.findall(pattern="<\$__uri__(.*?)>", string=s)
            )  # set(findall(pattern, string)) for distincts matches
            for u_number in uris_match:

                def simple_uuid_uri_generator() -> str:
                    return (
                        self.config["default_triples_root_uri"] + str(uuid.uuid4())[:8]
                    )

                urikey = f"__uri__{u_number}"
                uris[urikey] = simple_uuid_uri_generator()
            logger_t.debug(f"Created {len(uris_match)} URIs : {uris}")

            supdate = Template(s).substitute(kwargs | uris)
            logger_t.debug(f"--- --- SUBSTITUED TEMPLATE CONTENT --- ---\n{supdate}")

            self.g.update(supdate)
        return uris


class TripleStore(GraphStore):
    def __init__(self, config: dict | None = None, config_store: dict | None = None):
        """
        Keyword Args:
            config_store (dict): A dictionary of config settings for sparqlstore.SPARQLUpdateStore client
            config (dict): A dictionnary of config settings for Triplestore behaviour depending on its context (local/online)
                - ['path']
        """
        self.config = config or {}
        self.config_store = config_store or {}
        try:
            self.default_named_graph_uri = "".join(
                [
                    self.config["default_named_graph_root_uri"],
                    self.config["default_named_graph_name"],
                ]
            )
        except:
            raise ValueError(
                f"TripleStore Class API configuration error, cannot define default named graph with provided config, config={self.config}"
            )

    @property
    @cache
    def client(self):
        return sparqlstore.SPARQLUpdateStore(**self.config_store)

    # Select
    @GraphStore.raise_exceptions_query
    def select_templated(
        self,
        query_filename: str,
        format: str = "dataframe",
        override_named_graph_uri: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        query_path = Path(self.config["datapip_sparql_select_path"], query_filename)
        with open(query_path, "r", encoding="utf-8") as file:
            if len(kwargs) == 0:
                query_string = file.read()
            else:
                s = file.read()
                query_string = Template(s).substitute(
                    kwargs
                )  # squery = s.format(**kwargs)

        logger_t.debug(f"Select Query:\n{query_string}")

        named_graph = override_named_graph_uri or self.default_named_graph_uri
        g = Graph(
            store=self.client,
            identifier=named_graph,  # .lstrip('/'),
            bind_namespaces="none",
        )  # Do not load default rdflib namespaces, keep queries self-contained
        qr = g.query(query_string)

        match format:
            case "dataframe":
                return self.sparql_results_to_df(qr)
            case "json":
                return self.sparql_results_to_json(qr)
            case "dict":
                return self.sparql_results_to_dict(qr)
            case _:
                logger_t.warning(
                    f"Unrecoginzed format={format}, defaulting to 'dataframe' output"
                )
                return self.sparql_results_to_df(qr)

    @GraphStore.raise_exceptions_query
    def select_templated_parametrized(
        self,
        query_filename: str,
        format: str = "dataframe",
        enforce_parameters: Mapping[str, str] | None = None,
        override_named_graph_uri: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        query_path = Path(self.config["datapip_sparql_select_path"], query_filename)
        with open(query_path, "r", encoding="utf-8") as file:
            if len(kwargs) == 0:
                query_string = file.read()
            else:
                s = file.read()
                query_string = Template(s).substitute(
                    kwargs
                )  # squery = s.format(**kwargs)

        if enforce_parameters is not None:
                logger_t.debug(f"enforce={enforce_parameters}")
                enforce_parameters = {
                    f"?{k}": f"'{v}'"
                    for k, v in enforce_parameters.items()
                    if v is not None
                }  # escapes `None` values, add the `?` for wildcards replacements (cf SPARQL 1.1)
                logger_t.debug(f"enforcepost={enforce_parameters}")
                if enforce_parameters is not {}:
                    keys = (re.escape(k) for k in enforce_parameters.keys())
                    pattern = re.compile(r"(" + "".join(keys) + r")")
                    query_string = pattern.sub(
                        lambda x: enforce_parameters[x.group()], query_string
                    )

        logger_t.debug(f"Select Query:\n{query_string}")

        named_graph = override_named_graph_uri or self.default_named_graph_uri
        g = Graph(
            store=self.client,
            identifier=named_graph,  # .lstrip('/'),
            bind_namespaces="none",
        )  # Do not load default rdflib namespaces, keep queries self-contained
        qr = g.query(query_string)

        match format:
            case "dataframe":
                return self.sparql_results_to_df(qr)
            case "json":
                return self.sparql_results_to_json(qr)
            case "dict":
                return self.sparql_results_to_dict(qr)
            case _:
                logger_t.warning(
                    f"Unrecoginzed format={format}, defaulting to 'dataframe' output"
                )
                return self.sparql_results_to_df(qr)

    # Update
    def update_static(self, query_string: str):
        self.client.update(query_string, queryGraph=self.default_named_graph_uri)

    # @GraphStore.raise_exceptions_query
    def update_templated(self, query_filename: str, **kwargs) -> pd.DataFrame:
        try:
            query_path = Path(self.config["datapip_sparql_update_path"], query_filename)
        except KeyError as ke1:
            logger_t.error(ke1)
            logger_t.error(
                f"Incomplete config for select queries, add  'datapip_sparql_update_path' to your config={self.config}"
            )
            return ke1

        uris = {}
        with open(query_path, "r", encoding="utf-8") as file:
            s = file.read()

            uris_match = set(
                re.findall(pattern="<\$__uri__(.*?)>", string=s)
            )  # set(findall(pattern, string)) for distincts matches
            for u_number in uris_match:

                def simple_uuid_uri_generator() -> str:
                    return (
                        self.config["default_triples_root_uri"] + str(uuid.uuid4())[:8]
                    )

                urikey = f"__uri__{u_number}"
                uris[urikey] = simple_uuid_uri_generator()
            logger_t.debug(f"Created {len(uris_match)} URIs : {uris}")

            supdate = Template(s).substitute(kwargs | uris)
            logger_t.debug(f"--- --- SUBSTITUED TEMPLATE CONTENT --- ---\n{supdate}")

            self.client.update(supdate, queryGraph=self.default_named_graph_uri)
        return uris

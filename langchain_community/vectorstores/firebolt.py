from __future__ import annotations

import json
import logging
from hashlib import sha1
from threading import Thread
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


def has_mul_sub_str(s: str, *args: Any) -> bool:
    """
    Check if a string contains multiple substrings.
    Args:
        s: string to check.
        *args: substrings to check.

    Returns:
        True if all substrings are in the string, False otherwise.
    """
    for a in args:
        if a not in s:
            return False
    return True


class FireboltSettings(BaseSettings):
    """`Firebolt` client configuration.

    Attribute:
        firebolt_id (str) : Firebolt ID to login. Required.
        firebolt_secret (str) : Firebolt secret to login. Required.
        engine_name (str) : Firebolt engine name to use. Required.
        database (str) : Database name to find the table. Required.
        account_name (str) : Firebolt account name. Required.
        semantic_index (str) : Semantic index name to operate on. Defaults to 'pdf_semantic_index'.
        column_map (Dict) : Column type map to project column name onto langchain
                            semantics. Must have keys: `text`, `id`, `vector`,
                            must be same size to number of columns. For example:
                            .. code-block:: python

                                {
                                    'id': 'id',
                                    'embedding': 'embedding',
                                    'document': 'page_content'
                                }

                            Defaults to identity map.
    """

    firebolt_id: str
    firebolt_secret: str
    engine_name: str
    database: str
    account_name: str
    semantic_index: str
    llm_location: str
    embedding_model: str = "amazon.titan-embed-text-v2:0"
    embedding_dimensions: int = 256
    api_endpoint: Optional[str] = None  # Optional custom API endpoint

    column_map: Dict[str, str] = {
        "id": "id",
        "document": "page_content",
        "embedding": "embedding"        
    }

    metric: str = "cosine"

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="firebolt_",
        extra="ignore",
    )


class Firebolt(VectorStore):
    """Firebolt vector store integration.
    
    NOTE: This implementation only supports similarity_search operations.
    add_documents, add_texts, and delete methods are NOT IMPLEMENTED.
    Use external tools to populate and manage the Firebolt semantic index table.

    Setup:
        Install ``langchain_community`` and ``firebolt-sdk``:

        .. code-block:: bash

            pip install -qU langchain_community firebolt-sdk

    Key init args — client params:
        config: Optional[FireboltSettings]
            Firebolt client configuration.
        embedding: Optional[Embeddings]
            Embedding function to use (optional, uses AI_EMBED_TEXT SQL function if None).

    Instantiate:
        .. code-block:: python

            from langchain_community.vectorstores import Firebolt, FireboltSettings

            settings = FireboltSettings(
                firebolt_id="your_client_id",
                firebolt_secret="your_client_secret",
                database="my_database",
                semantic_index="pdf_semantic_index"
            )
            vector_store = Firebolt(config=settings)

    Search:
        .. code-block:: python

            results = vector_store.similarity_search(query="thud", k=1)
            for doc in results:
                print(f"* {doc.page_content} [{doc.metadata}]")

    Search with filter:
        .. code-block:: python

            results = vector_store.similarity_search(
                query="thud", k=1, filter="metadata->>'filename' = 'document.pdf'"
            )
            for doc in results:
                print(f"* {doc.page_content} [{doc.metadata}]")

    Search with score:
        .. code-block:: python

            results = vector_store.similarity_search_with_score(query="qux", k=1)
            for doc, score in results:
                print(f"* [SIM={score:3f}] {doc.page_content} [{doc.metadata}]")
    """

    def __init__(
        self,
        config: Optional[FireboltSettings] = None,
        **kwargs: Any,
    ) -> None:
        """Firebolt Wrapper to LangChain

        Args:
            config (FireboltSettings): Configuration to Firebolt Client
            kwargs (any): Other keyword arguments will pass into
                [firebolt-sdk](https://github.com/firebolt-db/firebolt-sdk-python)
        """
        try:
            from firebolt.client.auth import ClientCredentials
            from firebolt.db import Connection, connect
        except ImportError:
            raise ImportError(
                "Could not import firebolt-sdk python package. "
                "Please install it with `pip install firebolt-sdk`."
            )
        try:
            from tqdm import tqdm

            self.pgbar = tqdm
        except ImportError:
            # Just in case if tqdm is not installed
            self.pgbar = lambda x, **kwargs: x

        super().__init__()
        if config is not None:
            self.config = config
        else:
            self.config = FireboltSettings()
        assert self.config
        assert self.config.firebolt_id and self.config.firebolt_secret
        assert (
            self.config.column_map
            and self.config.database
            and self.config.semantic_index
            and self.config.metric
        )
        for k in ["id", "embedding", "document"]:
            assert k in self.config.column_map
        assert self.config.metric in ["cosine", "euclidean", "dot"]

        # Use AI_EMBED_TEXT for embeddings (dimensions from config)
        self.dim = self.config.embedding_dimensions
        self.use_sql_embeddings = True  # Always use SQL embeddings

        # Connect to Firebolt
        auth = ClientCredentials(
            client_id=self.config.firebolt_id,
            client_secret=self.config.firebolt_secret
        )
        connection_params = {
            "engine_name": self.config.engine_name,
            "database": self.config.database,
            "account_name": self.config.account_name,
            "auth": auth
        }
        # Add api_endpoint if specified (for custom domains/environments)
        # If api_endpoint contains "staging", use the staging API endpoint
        if self.config.api_endpoint:
            if "staging" in self.config.api_endpoint.lower():
                connection_params["api_endpoint"] = "https://api.staging.firebolt.io"
            else:
                connection_params["api_endpoint"] = self.config.api_endpoint
        self.connection = connect(**connection_params)
        self.client = self.connection
        
        # Set session settings to enable advanced functions
        cursor = self.connection.cursor()
        session_settings = [
            "SET advanced_mode=1",
            "SET enable_subresult_cache=false",
            "SET enable_vector_search_index_creation=1",
            "SET enable_vector_search_tvf=1",
            "SET enable_granule_pruning_by_tablet_id_and_row_number=1",
            "SET enable_udf_ddl=true"
        ]
        for setting in session_settings:
            try:
                cursor.execute(setting)
            except Exception as e:
                logger.warning(f"Failed to set session setting '{setting}': {e}")
        cursor.close()

        # Set distance ordering based on metric
        if self.config.metric == "cosine":
            self.dist_order = "ASC"  # Lower cosine distance = more similar
        elif self.config.metric == "euclidean":
            self.dist_order = "ASC"  # Lower euclidean distance = more similar
        elif self.config.metric == "dot":
            self.dist_order = "DESC"  # Higher dot product = more similar

        # Note: Table initialization is not performed.
        # The semantic index table should already exist and be populated externally.

    def _generate_embedding_sql(self, text: str) -> str:
        """Generate SQL query to create embedding using AI_EMBED_TEXT.
        
        Args:
            text: Text to generate embedding for
            
        Returns:
            SQL query string
        """
        location_clause = ""
        if self.config.llm_location:
            location_clause = f", LOCATION => '{self.config.llm_location}'"
        
        # Escape single quotes in text
        escaped_text = text.replace("'", "''")
        
        sql = f"""
        SELECT AI_EMBED_TEXT(
            MODEL => '{self.config.embedding_model}',
            INPUT_TEXT => '{escaped_text}',
            DIMENSIONS => {self.config.embedding_dimensions}
            {location_clause}
        ) AS embedding
        """
        return sql.strip()

    def _get_embedding(self, text: str) -> List[float]:
        """Get embedding for a text using AI_EMBED_TEXT SQL function.
        
        Args:
            text: Text to embed
            
        Returns:
            List of floats representing the embedding vector
        """
        
        # Use Firebolt AI_EMBED_TEXT
        cursor = self.connection.cursor()
        sql = self._generate_embedding_sql(text)
        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            if result and len(result) > 0:
                # Result is an array, convert to list
                embedding = result[0]
                if isinstance(embedding, (list, tuple)):
                    return list(embedding)
                # If it's a string representation, parse it
                elif isinstance(embedding, str):
                    import ast
                    return ast.literal_eval(embedding)
                else:
                    return list(embedding)
            else:
                raise ValueError(f"No embedding returned for text: {text[:50]}...")
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            raise
        finally:
            cursor.close()

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[Dict[Any, Any]]] = None,
        ids: Optional[Iterable[str]] = None,
        batch_size: int = 32,
        **kwargs: Any,
    ) -> List[str]:
        """Add texts to the vector store.
        
        NOT IMPLEMENTED: This method is not implemented. 
        Use external tools to populate the Firebolt semantic index table.

        Args:
            texts: Iterable of strings to add to the vector store.
            metadatas: Optional list of metadatas associated with the texts.
            ids: Optional list of unique IDs.
            batch_size: Batch size for insertion.
            kwargs: vectorstore specific parameters

        Returns:
            List of ids from adding the texts into the vectorstore.
        """
        raise NotImplementedError(
            "add_texts is not implemented. "
            "Please use external tools to populate the Firebolt semantic index table."
        )

    def add_documents(
        self,
        documents: List[Document],
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[str]:
        """Add documents to the vector store.
        
        NOT IMPLEMENTED: This method is not implemented.
        Use external tools to populate the Firebolt semantic index table.

        Args:
            documents: List of Document objects to add.
            ids: Optional list of IDs.
            kwargs: Additional arguments.

        Returns:
            List of IDs.
        """
        raise NotImplementedError(
            "add_documents is not implemented. "
            "Please use external tools to populate the Firebolt semantic index table."
        )

    @classmethod
    def from_texts(
        cls,
        texts: List[str],
        embedding: Optional[Embeddings] = None,
        metadatas: Optional[List[Dict[Any, Any]]] = None,
        config: Optional[FireboltSettings] = None,
        text_ids: Optional[Iterable[str]] = None,
        batch_size: int = 32,
        **kwargs: Any,
    ) -> Firebolt:
        """Create Firebolt wrapper with existing texts
        
        NOT IMPLEMENTED: This method is not implemented.
        Use external tools to populate the Firebolt semantic index table.

        Args:
            embedding (Optional[Embeddings]): Function to extract text embedding (optional, uses AI_EMBED_TEXT if None)
            texts (Iterable[str]): List or tuple of strings to be added
            config (FireboltSettings, Optional): Firebolt configuration
            text_ids (Optional[Iterable], optional): IDs for the texts.
                                                     Defaults to None.
            batch_size (int, optional): Batchsize when transmitting data to Firebolt.
                                        Defaults to 32.
            metadata (List[dict], optional): metadata to texts. Defaults to None.
            Other keyword arguments will pass into
                [firebolt-sdk](https://github.com/firebolt-db/firebolt-sdk-python)
        Returns:
            Firebolt Index
        """
        raise NotImplementedError(
            "from_texts is not implemented. "
            "Please use external tools to populate the Firebolt semantic index table."
        )

    def __repr__(self) -> str:
        """Text representation for Firebolt Vector Store, prints backends, username
            and schemas. Easy to use with `str(Firebolt())`

        Returns:
            repr: string to show connection info and data schema
        """
        _repr = f"\033[92m\033[1m{self.config.database}.{self.config.semantic_index}\033[0m\n\n"
        _repr += f"\033[1mEngine: {self.config.engine_name}\033[0m\n"
        _repr += f"\033[1mAccount: {self.config.account_name}\033[0m\n"
        _repr += f"\033[1mSemantic Index: {self.config.semantic_index}\033[0m\n"
        return _repr

    def _build_query_sql(
        self, q_emb: List[float], topk: int
    ) -> str:
        """Construct an SQL query for performing a similarity search.

        This internal method generates an SQL query for finding the top-k most similar
        vectors in the database to a given query vector. It allows for optional filtering
        conditions to be applied via a WHERE clause.

        Args:
            q_emb: The query vector as a list of floats.
            topk: The number of top similar items to retrieve.

        Returns:
            A string containing the SQL query for the similarity search.
        """
        q_emb_str = ",".join(map(str, q_emb))

    
        
        distance_func = f"VECTOR_COSINE_DISTANCE({self.config.column_map['embedding']}, [{q_emb_str}])"
        
        q_str = f"""
            SELECT 
                {self.config.column_map['document']}, 
                {distance_func} AS dist
            FROM vector_search( INDEX {self.config.semantic_index}, [{q_emb_str}], {topk}, 16 )
            ORDER BY dist {self.dist_order}
        """
        return q_str

    def similarity_search(
        self, query: str, k: int = 20, **kwargs: Any
    ) -> List[Document]:
        """Perform a similarity search with Firebolt

        Args:
            query (str): query string
            k (int, optional): Top K neighbors to retrieve. Defaults to 4.
            filter (Optional[str], optional): WHERE condition string.
                                             Defaults to None.

            NOTE: Please do not let end-user to fill this and always be aware
                  of SQL injection. When dealing with metadatas, remember to
                  use JSON extraction functions like `metadata->>'key'` for
                  filtering.

        Returns:
            List[Document]: List of Documents
        """
        # Generate query embedding using AI_EMBED_TEXT or LangChain function
        query_embedding = self._get_embedding(query)
        
        return self.similarity_search_by_vector(query_embedding, k, **kwargs)

    def similarity_search_by_vector(
        self,
        embedding: List[float],
        k: int = 20,
        **kwargs: Any,
    ) -> List[Document]:
        """Perform a similarity search with Firebolt by vectors

        Args:
            embedding (List[float]): query vector
            k (int, optional): Top K neighbors to retrieve. Defaults to 4.
            filter (Optional[str], optional): WHERE condition string.
                                             Defaults to None.

            NOTE: Please do not let end-user to fill this and always be aware
                  of SQL injection. When dealing with metadatas, remember to
                  use JSON extraction functions like `metadata->>'key'` for
                  filtering.

        Returns:
            List[Document]: List of documents
        """
        q_str = self._build_query_sql(embedding, k)
        cursor = self.connection.cursor()
        try:
            # Execute SET statements before the SELECT query
            cursor.execute("SET advanced_mode=1")
            cursor.execute("SET enable_vector_search_tvf=1")
            # Now execute the SELECT query
            cursor.execute(q_str)
            results = []
            for row in cursor.fetchall():
                doc_content = row[0]
                results.append(
                    Document(
                        page_content=doc_content,
                    )
                )
            return results
        except Exception as e:
            logger.error(f"\033[91m\033[1m{type(e)}\033[0m \033[95m{str(e)}\033[0m")
            return []
        finally:
            cursor.close()

    def similarity_search_with_score(
        self, query: str, k: int = 20, **kwargs: Any
    ) -> List[Tuple[Document, float]]:
        """Perform a similarity search with Firebolt

        Args:
            query (str): query string
            k (int, optional): Top K neighbors to retrieve. Defaults to 4.
            filter (Optional[str], optional): WHERE condition string.
                                             Defaults to None.

            NOTE: Please do not let end-user to fill this and always be aware
                  of SQL injection. When dealing with metadatas, remember to
                  use JSON extraction functions like `metadata->>'key'` for
                  filtering.

        Returns:
            List[Tuple[Document, float]]: List of (Document, similarity score)
        """
        # Generate query embedding using AI_EMBED_TEXT or LangChain function
       
        query_embedding = self._get_embedding(query)
        q_str = self._build_query_sql(query_embedding, k)
        cursor = self.connection.cursor()
        try:
            # Execute SET statements before the SELECT query
            cursor.execute("SET advanced_mode=1")
            cursor.execute("SET enable_vector_search_tvf=1")
            # Now execute the SELECT query
            cursor.execute(q_str)
            results = []
            for row in cursor.fetchall():
                doc_content = row[0]
                score = float(row[1]) if len(row) > 1 else 0.0
                results.append(
                    (
                        Document(
                            page_content=doc_content,
                        ),
                        score,
                    )
                )
            return results
        except Exception as e:
            logger.error(f"\033[91m\033[1m{type(e)}\033[0m \033[95m{str(e)}\033[0m")
            return []
        finally:
            cursor.close()

    def delete(self, ids: Optional[List[str]] = None, **kwargs: Any) -> Optional[bool]:
        """
        Delete by vector IDs.
        
        NOT IMPLEMENTED: This method is not implemented.
        Use external tools to delete from the Firebolt semantic index table.

        Args:
            ids: List of ids to delete.

        Returns:
            Optional[bool]: True if deletion was successful.
        """
        raise NotImplementedError(
            "delete is not implemented. "
            "Please use external tools to delete from the Firebolt semantic index table."
        )

    def drop(self) -> None:
        """
        Helper function: Drop data
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"DROP TABLE IF EXISTS {self.config.database}.{self.config.semantic_index}"
            )
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error dropping table: {e}")
            self.connection.rollback()
        finally:
            cursor.close()

    @property
    def metadata_column(self) -> str:
        # Metadata column not used in current implementation
        return "metadata"


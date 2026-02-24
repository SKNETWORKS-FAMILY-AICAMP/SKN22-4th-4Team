"""
GraphRAG implementation using Neo4j for graph queries and Supabase for company data
Hybrid architecture: Neo4j for relationships, Supabase for general data
"""

import os
import json
import logging
from typing import List, Dict, Optional
import networkx as nx
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Neo4j 드라이버 임포트
try:
    from neo4j import GraphDatabase

    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    logger.warning("neo4j driver not installed. Install with: pip install neo4j")

# LLM Client 임포트
try:
    from rag.llm_client import get_llm_client
except ImportError:
    try:
        from src.rag.llm_client import get_llm_client
    except ImportError:
        get_llm_client = None


class GraphRAG:
    """
    Graph-based RAG using Neo4j + Supabase:
    - Neo4j: 회사 간 관계 그래프 (Cypher 쿼리)
    - Supabase: companies 테이블 (회사 정보), documents 테이블 (벡터 문서)
    """

    def __init__(
        self,
        embedding_model: str = "text-embedding-3-small",
        llm_model: Optional[str] = None,
    ):
        """Initialize GraphRAG with Neo4j + Supabase"""

        # LLM Client
        self.llm_model = llm_model or os.getenv("CHAT_MODEL", "gemini-2.5-flash")
        self.llm_client = None
        if get_llm_client:
            try:
                self.llm_client = get_llm_client(self.llm_model)
            except Exception as e:
                logger.warning(f"LLM client init failed: {e}")

        self.embedding_model = embedding_model

        # Neo4j client
        self.neo4j_driver = None
        if NEO4J_AVAILABLE:
            neo4j_uri = os.getenv("NEO4J_URI")
            neo4j_user = os.getenv("NEO4J_USER")
            neo4j_password = os.getenv("NEO4J_PASSWORD")

            if neo4j_uri and neo4j_user and neo4j_password:
                try:
                    self.neo4j_driver = GraphDatabase.driver(
                        neo4j_uri, auth=(neo4j_user, neo4j_password)
                    )
                    # 연결 테스트
                    self.neo4j_driver.verify_connectivity()
                    logger.info("Neo4j connected successfully")
                except Exception as e:
                    logger.warning(f"Neo4j connection failed: {e}")
                    self.neo4j_driver = None
            else:
                logger.warning("Neo4j credentials not found in .env")

        # Supabase client (회사 정보용)
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL과 SUPABASE_KEY 환경 변수가 필요합니다.")

        self.supabase: Client = create_client(supabase_url, supabase_key)

        # Local graph for analysis (NetworkX)
        self.local_graph = nx.DiGraph()

        source = "Neo4j" if self.neo4j_driver else "Supabase"
        logger.info(f"GraphRAG initialized (relationships: {source})")

    def _llm_chat(self, system_prompt: str, user_prompt: str) -> str:
        """LLM 호출 (Gemini 우선)"""
        if self.llm_client:
            return self.llm_client.chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
            )
        else:
            raise RuntimeError("No LLM client available for GraphRAG")

    def _neo4j_query(self, query: str, parameters: dict = None) -> List[Dict]:
        """Execute a Neo4j Cypher query and return results"""
        if not self.neo4j_driver:
            return []
        try:
            with self.neo4j_driver.session() as session:
                result = session.run(query, parameters or {})
                return [record.data() for record in result]
        except Exception as e:
            logger.error(f"Neo4j query error: {e}")
            return []

    def extract_relationships(
        self, text: str, source_ticker: Optional[str] = None
    ) -> List[Dict]:
        """Extract company relationships from text using LLM"""

        system_prompt = """You are a financial analyst. Extract company relationships from text.

Relationship types: partnership, acquisition, supplier, customer, competitor, subsidiary, investment

Return JSON only:
[{"source_company": "...", "source_ticker": "...", "target_company": "...", "target_ticker": "...", 
  "relationship_type": "...", "confidence": 0.8}]"""

        user_prompt = f"Source Company Ticker: {source_ticker or 'Unknown'}\n\nText:\n{text[:3000]}"

        try:
            response = self._llm_chat(system_prompt, user_prompt)

            # Clean JSON
            response = response.strip()
            if response.startswith("```"):
                response = response.split("```")[1]
                if response.startswith("json"):
                    response = response[4:]

            return json.loads(response.strip())

        except Exception as e:
            logger.error(f"Extraction error: {e}")
            return []

    def save_relationships(
        self,
        relationships: List[Dict],
        extracted_from: str = None,
        filing_date: str = None,
    ) -> int:
        """Save relationships to Neo4j (primary) or Supabase (fallback)"""
        if not relationships:
            return 0

        if self.neo4j_driver:
            return self._save_to_neo4j(relationships)
        else:
            return self._save_to_supabase(relationships, extracted_from, filing_date)

    def _save_to_neo4j(self, relationships: List[Dict]) -> int:
        """Save relationships to Neo4j"""
        saved = 0
        for rel in relationships:
            source_ticker = rel.get("source_ticker", "")
            target_ticker = rel.get("target_ticker", "")

            if not source_ticker or not target_ticker:
                continue

            rel_type = (
                rel.get("relationship_type", "RELATED")
                .upper()
                .replace(" ", "_")
                .replace("-", "_")
            )

            query = f"""
            MERGE (source:Company {{ticker: $source_ticker}})
            ON CREATE SET source.name = $source_name
            MERGE (target:Company {{ticker: $target_ticker}})
            ON CREATE SET target.name = $target_name
            MERGE (source)-[r:`{rel_type}`]->(target)
            SET r.confidence = $confidence,
                r.original_type = $original_type
            """

            results = self._neo4j_query(
                query,
                {
                    "source_ticker": source_ticker,
                    "source_name": rel.get("source_company", source_ticker),
                    "target_ticker": target_ticker,
                    "target_name": rel.get("target_company", target_ticker),
                    "confidence": rel.get("confidence", 0.5),
                    "original_type": rel.get("relationship_type", "related"),
                },
            )
            saved += 1

        return saved

    def _save_to_supabase(
        self, relationships: List[Dict], extracted_from=None, filing_date=None
    ) -> int:
        """Save relationships to Supabase (fallback)"""
        records = []
        for rel in relationships:
            records.append(
                {
                    "source_company": rel.get("source_company", ""),
                    "source_ticker": rel.get("source_ticker", ""),
                    "target_company": rel.get("target_company", ""),
                    "target_ticker": rel.get("target_ticker", ""),
                    "relationship_type": rel.get("relationship_type", "related"),
                    "confidence": rel.get("confidence", 0.5),
                    "extracted_from": extracted_from,
                    "filing_date": filing_date,
                }
            )

        try:
            self.supabase.table("company_relationships").insert(records).execute()
            return len(records)
        except Exception as e:
            logger.error(f"Error saving relationships to Supabase: {e}")
            return 0

    def find_relationships(
        self, ticker: str, relationship_type: Optional[str] = None
    ) -> Dict:
        """Find relationships for a company by ticker"""
        if self.neo4j_driver:
            return self._find_relationships_neo4j(ticker, relationship_type)
        else:
            return self._find_relationships_supabase(ticker, relationship_type)

    def _find_relationships_neo4j(
        self, ticker: str, relationship_type: Optional[str] = None
    ) -> Dict:
        """Find relationships using Neo4j Cypher"""
        try:
            # Outgoing relationships
            outgoing_query = """
            MATCH (source:Company {ticker: $ticker})-[r]->(target:Company)
            RETURN source.name AS source_company, source.ticker AS source_ticker,
                   type(r) AS relationship_type, r.confidence AS confidence,
                   r.original_type AS original_type,
                   target.name AS target_company, target.ticker AS target_ticker
            """

            # Incoming relationships
            incoming_query = """
            MATCH (source:Company)-[r]->(target:Company {ticker: $ticker})
            RETURN source.name AS source_company, source.ticker AS source_ticker,
                   type(r) AS relationship_type, r.confidence AS confidence,
                   r.original_type AS original_type,
                   target.name AS target_company, target.ticker AS target_ticker
            """

            outgoing = self._neo4j_query(outgoing_query, {"ticker": ticker})
            incoming = self._neo4j_query(incoming_query, {"ticker": ticker})

            if relationship_type:
                rt_upper = relationship_type.upper().replace(" ", "_")
                outgoing = [
                    r for r in outgoing if r.get("relationship_type") == rt_upper
                ]
                incoming = [
                    r for r in incoming if r.get("relationship_type") == rt_upper
                ]

            return {
                "ticker": ticker,
                "outgoing": outgoing,
                "incoming": incoming,
                "total": len(outgoing) + len(incoming),
            }

        except Exception as e:
            logger.error(f"Neo4j find_relationships error: {e}")
            return {"ticker": ticker, "outgoing": [], "incoming": [], "error": str(e)}

    def _find_relationships_supabase(
        self, ticker: str, relationship_type: Optional[str] = None
    ) -> Dict:
        """Find relationships using Supabase (fallback)"""
        try:
            query = (
                self.supabase.table("company_relationships")
                .select("*")
                .eq("source_ticker", ticker)
            )
            if relationship_type:
                query = query.eq("relationship_type", relationship_type)
            outgoing = query.execute().data

            query = (
                self.supabase.table("company_relationships")
                .select("*")
                .eq("target_ticker", ticker)
            )
            if relationship_type:
                query = query.eq("relationship_type", relationship_type)
            incoming = query.execute().data

            return {
                "ticker": ticker,
                "outgoing": outgoing,
                "incoming": incoming,
                "total": len(outgoing) + len(incoming),
            }

        except Exception as e:
            logger.error(f"Supabase find_relationships error: {e}")
            return {"ticker": ticker, "outgoing": [], "incoming": [], "error": str(e)}

    def get_company(self, ticker: str) -> Optional[Dict]:
        """Get company info by ticker (from Supabase)"""
        try:
            result = (
                self.supabase.table("companies")
                .select("*")
                .eq("ticker", ticker)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Error getting company: {e}")
            return None

    def search_companies(self, query: str, limit: int = 10) -> List[Dict]:
        """Search companies by name"""
        try:
            result = (
                self.supabase.table("companies")
                .select("*")
                .ilike("company_name", f"%{query}%")
                .limit(limit)
                .execute()
            )
            return result.data
        except Exception as e:
            logger.error(f"Error searching companies: {e}")
            return []

    def get_company_network(self, ticker: str, depth: int = 1) -> Dict:
        """Get company relationship network"""
        if self.neo4j_driver:
            return self._get_network_neo4j(ticker, depth)
        else:
            return self._get_network_traversal(ticker, depth)

    def _get_network_neo4j(self, ticker: str, depth: int) -> Dict:
        """Get network using Neo4j variable-length path"""
        query = (
            """
        MATCH path = (start:Company {ticker: $ticker})-[*1..%d]-(connected:Company)
        UNWIND relationships(path) AS r
        WITH startNode(r) AS s, endNode(r) AS t, type(r) AS rel_type
        RETURN DISTINCT s.ticker AS source, s.name AS source_name,
               t.ticker AS target, t.name AS target_name,
               rel_type AS type
        LIMIT 100
        """
            % depth
        )

        results = self._neo4j_query(query, {"ticker": ticker})

        nodes = {}
        edges = []

        for r in results:
            src = r.get("source")
            tgt = r.get("target")
            if src and src not in nodes:
                nodes[src] = {
                    "id": src,
                    "name": r.get("source_name", src),
                    "sector": "",
                }
            if tgt and tgt not in nodes:
                nodes[tgt] = {
                    "id": tgt,
                    "name": r.get("target_name", tgt),
                    "sector": "",
                }
            edges.append(
                {
                    "source": src,
                    "target": tgt,
                    "type": r.get("type", "related"),
                }
            )

        return {"nodes": list(nodes.values()), "edges": edges}

    def _get_network_traversal(self, ticker: str, depth: int) -> Dict:
        """Get network using BFS traversal (Supabase fallback)"""
        visited = set()
        network = {"nodes": [], "edges": []}

        def traverse(current_ticker: str, current_depth: int):
            if current_depth > depth or current_ticker in visited:
                return
            visited.add(current_ticker)

            company = self.get_company(current_ticker)
            if company:
                network["nodes"].append(
                    {
                        "id": current_ticker,
                        "name": company.get("company_name", current_ticker),
                        "sector": company.get("sector", ""),
                    }
                )

            rels = self.find_relationships(current_ticker)

            for rel in rels.get("outgoing", []):
                target = rel.get("target_ticker")
                if target:
                    network["edges"].append(
                        {
                            "source": current_ticker,
                            "target": target,
                            "type": rel.get("relationship_type", "related"),
                        }
                    )
                    traverse(target, current_depth + 1)

            for rel in rels.get("incoming", []):
                source = rel.get("source_ticker")
                if source:
                    network["edges"].append(
                        {
                            "source": source,
                            "target": current_ticker,
                            "type": rel.get("relationship_type", "related"),
                        }
                    )
                    traverse(source, current_depth + 1)

        traverse(ticker, 0)
        return network

    def query_with_context(self, query: str, ticker: Optional[str] = None) -> Dict:
        """Query with relationship context"""

        context_parts = []

        if ticker:
            company = self.get_company(ticker)
            if company:
                context_parts.append(
                    f"Company: {company.get('company_name')} ({ticker})"
                )
                context_parts.append(f"Sector: {company.get('sector', 'N/A')}")
                context_parts.append(f"Industry: {company.get('industry', 'N/A')}")

            rels = self.find_relationships(ticker)
            if rels["total"] > 0:
                context_parts.append("\nRelationships:")
                for rel in rels.get("outgoing", [])[:10]:
                    context_parts.append(
                        f"  → {rel.get('relationship_type', 'related')}: "
                        f"{rel.get('target_company', '')} ({rel.get('target_ticker', '')})"
                    )
                for rel in rels.get("incoming", [])[:10]:
                    context_parts.append(
                        f"  ← {rel.get('relationship_type', 'related')}: "
                        f"{rel.get('source_company', '')} ({rel.get('source_ticker', '')})"
                    )

        context_str = (
            "\n".join(context_parts)
            if context_parts
            else "No specific context available."
        )

        system_prompt = """You are a financial analyst assistant. Answer based on the company and relationship context.
Be specific and cite relationships when relevant. Answer in Korean."""

        user_prompt = f"Context:\n{context_str}\n\nQuestion: {query}"

        response = self._llm_chat(system_prompt, user_prompt)

        return {
            "query": query,
            "ticker": ticker,
            "response": response,
            "context": context_str,
        }

    def build_local_graph(self) -> int:
        """
        Neo4j 또는 Supabase의 관계 데이터를 NetworkX 그래프로 로드합니다.
        Returns: 로드된 엣지 수
        """
        try:
            self.local_graph.clear()

            if self.neo4j_driver:
                # Neo4j에서 모든 관계 가져오기
                query = """
                MATCH (s:Company)-[r]->(t:Company)
                RETURN s.ticker AS source, s.name AS source_name,
                       t.ticker AS target, t.name AS target_name,
                       type(r) AS rel_type, r.confidence AS confidence
                """
                relationships = self._neo4j_query(query)
            else:
                # Supabase fallback
                result = (
                    self.supabase.table("company_relationships").select("*").execute()
                )
                relationships = [
                    {
                        "source": r.get("source_ticker"),
                        "source_name": r.get("source_company"),
                        "target": r.get("target_ticker"),
                        "target_name": r.get("target_company"),
                        "rel_type": r.get("relationship_type", "related"),
                        "confidence": r.get("confidence", 0.5),
                    }
                    for r in (result.data or [])
                ]

            for rel in relationships:
                source = rel.get("source")
                target = rel.get("target")
                rel_type = rel.get("rel_type", "related")
                confidence = rel.get("confidence", 0.5) or 0.5

                if source and target:
                    self.local_graph.add_node(
                        source, name=rel.get("source_name", source)
                    )
                    self.local_graph.add_node(
                        target, name=rel.get("target_name", target)
                    )
                    self.local_graph.add_edge(
                        source,
                        target,
                        relationship_type=rel_type,
                        confidence=confidence,
                        weight=1 - confidence,
                    )

            logger.info(
                f"Built local graph: {self.local_graph.number_of_nodes()} nodes, "
                f"{self.local_graph.number_of_edges()} edges"
            )
            return self.local_graph.number_of_edges()

        except Exception as e:
            logger.error(f"Error building local graph: {e}")
            return 0

    def get_centrality(self, top_n: int = 10) -> Dict:
        """중심성 분석 - 가장 영향력 있는 기업 찾기"""
        if self.local_graph.number_of_nodes() == 0:
            self.build_local_graph()

        if self.local_graph.number_of_nodes() == 0:
            return {"error": "그래프에 데이터가 없습니다."}

        try:
            degree_cent = nx.degree_centrality(self.local_graph)
            betweenness_cent = nx.betweenness_centrality(self.local_graph)

            try:
                closeness_cent = nx.closeness_centrality(self.local_graph)
            except:
                closeness_cent = {}

            top_degree = sorted(degree_cent.items(), key=lambda x: x[1], reverse=True)[
                :top_n
            ]
            top_betweenness = sorted(
                betweenness_cent.items(), key=lambda x: x[1], reverse=True
            )[:top_n]
            top_closeness = sorted(
                closeness_cent.items(), key=lambda x: x[1], reverse=True
            )[:top_n]

            return {
                "degree_centrality": [
                    {"ticker": k, "score": round(v, 4)} for k, v in top_degree
                ],
                "betweenness_centrality": [
                    {"ticker": k, "score": round(v, 4)} for k, v in top_betweenness
                ],
                "closeness_centrality": [
                    {"ticker": k, "score": round(v, 4)} for k, v in top_closeness
                ],
                "total_nodes": self.local_graph.number_of_nodes(),
                "total_edges": self.local_graph.number_of_edges(),
            }

        except Exception as e:
            logger.error(f"Error calculating centrality: {e}")
            return {"error": str(e)}

    def find_shortest_path(self, source_ticker: str, target_ticker: str) -> Dict:
        """두 기업 간의 최단 경로 - Neo4j 우선, NetworkX 폴백"""
        if self.neo4j_driver:
            return self._shortest_path_neo4j(source_ticker, target_ticker)
        else:
            return self._shortest_path_networkx(source_ticker, target_ticker)

    def _shortest_path_neo4j(self, source_ticker: str, target_ticker: str) -> Dict:
        """Neo4j shortestPath() 사용"""
        query = """
        MATCH (start:Company {ticker: $source}), (end:Company {ticker: $target}),
              path = shortestPath((start)-[*..10]-(end))
        UNWIND relationships(path) AS r
        RETURN startNode(r).ticker AS from_ticker,
               startNode(r).name AS from_name,
               endNode(r).ticker AS to_ticker,
               endNode(r).name AS to_name,
               type(r) AS relationship,
               length(path) AS path_length
        """
        results = self._neo4j_query(
            query, {"source": source_ticker, "target": target_ticker}
        )

        if not results:
            return {
                "error": f"'{source_ticker}'와 '{target_ticker}' 사이에 경로가 없습니다."
            }

        path = [source_ticker]
        details = []
        for r in results:
            to_ticker = r.get("to_ticker")
            if to_ticker and to_ticker not in path:
                path.append(to_ticker)
            details.append(
                {
                    "from": r.get("from_ticker"),
                    "to": to_ticker,
                    "direction": "→",
                    "relationship": r.get("relationship", "related"),
                }
            )

        return {
            "source": source_ticker,
            "target": target_ticker,
            "path": path,
            "path_length": len(path) - 1,
            "details": details,
        }

    def _shortest_path_networkx(self, source_ticker: str, target_ticker: str) -> Dict:
        """NetworkX 사용 (폴백)"""
        if self.local_graph.number_of_nodes() == 0:
            self.build_local_graph()

        try:
            undirected = self.local_graph.to_undirected()

            if source_ticker not in undirected or target_ticker not in undirected:
                return {
                    "error": f"'{source_ticker}' 또는 '{target_ticker}'가 그래프에 없습니다."
                }

            path = nx.shortest_path(
                undirected, source=source_ticker, target=target_ticker
            )

            path_details = []
            for i in range(len(path) - 1):
                node1, node2 = path[i], path[i + 1]

                if self.local_graph.has_edge(node1, node2):
                    edge_data = self.local_graph.get_edge_data(node1, node2)
                    direction = "→"
                elif self.local_graph.has_edge(node2, node1):
                    edge_data = self.local_graph.get_edge_data(node2, node1)
                    direction = "←"
                else:
                    edge_data = {}
                    direction = "—"

                path_details.append(
                    {
                        "from": node1,
                        "to": node2,
                        "direction": direction,
                        "relationship": edge_data.get("relationship_type", "related"),
                    }
                )

            return {
                "source": source_ticker,
                "target": target_ticker,
                "path": path,
                "path_length": len(path) - 1,
                "details": path_details,
            }

        except nx.NetworkXNoPath:
            return {
                "error": f"'{source_ticker}'와 '{target_ticker}' 사이에 경로가 없습니다."
            }
        except Exception as e:
            logger.error(f"Error finding shortest path: {e}")
            return {"error": str(e)}

    def get_connected_companies(self, ticker: str, max_depth: int = 2) -> Dict:
        """특정 기업과 연결된 모든 기업 찾기"""
        if self.neo4j_driver:
            query = (
                """
            MATCH path = (start:Company {ticker: $ticker})-[*1..%d]-(connected:Company)
            WHERE connected <> start
            WITH connected.ticker AS ticker, min(length(path)) AS min_depth
            RETURN ticker, min_depth
            ORDER BY min_depth, ticker
            """
                % max_depth
            )

            results = self._neo4j_query(query, {"ticker": ticker})

            connected_by_depth = {}
            for r in results:
                d = r.get("min_depth", 1)
                key = f"depth_{d}"
                if key not in connected_by_depth:
                    connected_by_depth[key] = []
                connected_by_depth[key].append(r.get("ticker"))

            return {
                "ticker": ticker,
                "connected": connected_by_depth,
                "total_connected": len(results),
            }
        else:
            # NetworkX fallback
            if self.local_graph.number_of_nodes() == 0:
                self.build_local_graph()

            if ticker not in self.local_graph:
                return {"error": f"'{ticker}'가 그래프에 없습니다."}

            try:
                undirected = self.local_graph.to_undirected()
                connected_by_depth = {}
                visited = {ticker}
                current_level = {ticker}

                for depth in range(1, max_depth + 1):
                    next_level = set()
                    for node in current_level:
                        neighbors = set(undirected.neighbors(node)) - visited
                        next_level.update(neighbors)
                        visited.update(neighbors)

                    if next_level:
                        connected_by_depth[f"depth_{depth}"] = list(next_level)
                    current_level = next_level

                return {
                    "ticker": ticker,
                    "connected": connected_by_depth,
                    "total_connected": len(visited) - 1,
                }

            except Exception as e:
                logger.error(f"Error finding connected companies: {e}")
                return {"error": str(e)}

    def get_stats(self) -> Dict:
        """Get statistics"""
        stats = {}

        try:
            # Neo4j stats
            if self.neo4j_driver:
                node_result = self._neo4j_query(
                    "MATCH (n:Company) RETURN count(n) AS count"
                )
                rel_result = self._neo4j_query(
                    "MATCH ()-[r]->() RETURN count(r) AS count"
                )
                stats["neo4j_nodes"] = node_result[0]["count"] if node_result else 0
                stats["neo4j_relationships"] = (
                    rel_result[0]["count"] if rel_result else 0
                )

            # Supabase stats
            companies = (
                self.supabase.table("companies").select("id", count="exact").execute()
            )
            documents = (
                self.supabase.table("documents").select("id", count="exact").execute()
            )

            stats["companies"] = companies.count or 0
            stats["documents"] = documents.count or 0

        except Exception as e:
            stats["error"] = str(e)

        return stats

    def close(self):
        """리소스 정리"""
        if self.neo4j_driver:
            self.neo4j_driver.close()
            logger.info("Neo4j driver closed")


# LangGraph Tool function
def graph_search_tool(query: str, ticker: str = None) -> str:
    """
    회사 관계 그래프에서 정보를 검색합니다.
    LangGraph Tool로 사용됩니다.
    """
    try:
        graph_rag = GraphRAG()
        result = graph_rag.query_with_context(query, ticker)
        return result.get("response", "관련 정보를 찾을 수 없습니다.")
    except Exception as e:
        logger.error(f"Graph search error: {e}")
        return f"검색 오류: {e}"


if __name__ == "__main__":
    print("🔄 GraphRAG 초기화 중...")

    try:
        graph_rag = GraphRAG()
        stats = graph_rag.get_stats()

        print(f"✅ GraphRAG 초기화 성공!")
        print(f"   LLM Model: {graph_rag.llm_model}")
        print(f"   Neo4j: {'Connected' if graph_rag.neo4j_driver else 'Not available'}")

        if "neo4j_nodes" in stats:
            print(f"   Neo4j Nodes: {stats.get('neo4j_nodes', 'N/A')}")
            print(f"   Neo4j Relationships: {stats.get('neo4j_relationships', 'N/A')}")

        print(f"   Supabase Companies: {stats.get('companies', 'N/A')}")
        print(f"   Supabase Documents: {stats.get('documents', 'N/A')}")

        if "error" in stats:
            print(f"   ⚠️ Error: {stats['error']}")

        # 관계 테스트
        print("\n🔍 AAPL 관계 검색...")
        rels = graph_rag.find_relationships("AAPL")
        print(f"   총 관계: {rels['total']}개")
        for rel in rels.get("outgoing", [])[:3]:
            print(f"   → {rel.get('relationship_type')}: {rel.get('target_company')}")

    except Exception as e:
        print(f"❌ 오류: {e}")

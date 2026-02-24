import os
import sys
import logging
from tqdm import tqdm
from dotenv import load_dotenv
from neo4j import GraphDatabase
from supabase import create_client, Client

# Add src to path if needed to run standalone
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def migrate_to_neo4j():
    logger.info("Starting migration from Supabase to Neo4j...")

    # 1. Initialize Supabase Client
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        logger.error("Missing SUPABASE_URL or SUPABASE_KEY in .env")
        return

    supabase: Client = create_client(supabase_url, supabase_key)
    logger.info("Supabase client initialized.")

    # 2. Initialize Neo4j Client
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    if not neo4j_uri or not neo4j_password:
        logger.error("Missing NEO4J_URI or NEO4J_PASSWORD in .env")
        return

    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        # Verify connection
        driver.verify_connectivity()
        logger.info("Neo4j driver initialized and connected.")
    except Exception as e:
        logger.error(f"Failed to connect to Neo4j: {e}")
        return

    # 3. Fetch Companies from Supabase
    logger.info("Fetching companies from Supabase...")
    try:
        # Fetching all companies. If you have >1000, pagination might be needed.
        companies_response = supabase.table("companies").select("*").execute()
        companies_data = companies_response.data
        logger.info(f"Found {len(companies_data)} companies in Supabase.")
    except Exception as e:
        logger.error(f"Error fetching companies: {e}")
        return

    # 4. Fetch Relationships from Supabase
    logger.info("Fetching relationships from Supabase...")
    try:
        rels_response = supabase.table("company_relationships").select("*").execute()
        rels_data = rels_response.data
        logger.info(f"Found {len(rels_data)} relationships in Supabase.")
    except Exception as e:
        logger.error(f"Error fetching relationships: {e}")
        return

    # 5. Migrate Data to Neo4j
    logger.info("Migrating data to Neo4j...")
    try:
        with driver.session() as session:
            # 5-1. Create Constraints (optional but recommended for performance and integrity)
            logger.info("Setting up database constraints...")
            # Using Cypher 5 syntax for constraints
            session.run(
                "CREATE CONSTRAINT company_ticker IF NOT EXISTS FOR (c:Company) REQUIRE c.ticker IS UNIQUE"
            )

            # 5-2. Migrate Companies (Nodes)
            logger.info("Creating Company nodes...")

            def create_company_nodes(tx, companies):
                query = """
                UNWIND $companies AS comp
                MERGE (c:Company {ticker: comp.ticker})
                SET c.name = comp.company_name,
                    c.sector = comp.sector,
                    c.industry = comp.industry,
                    c.cik = comp.cik
                """
                tx.run(query, companies=companies)

            session.execute_write(create_company_nodes, companies_data)
            logger.info(f"Successfully created {len(companies_data)} Company nodes.")

            # 5-3. Migrate Relationships (Edges)
            logger.info("Creating relationships...")

            def create_relationships(tx, relationships):
                # We use UNWIND for bulk insert
                query = """
                UNWIND $rels AS rel
                
                // Ensure nodes exist first (in case relationship data has tickers not in companies table)
                MERGE (source:Company {ticker: rel.source_ticker})
                ON CREATE SET source.name = coalesce(rel.source_company, rel.source_ticker)
                
                MERGE (target:Company {ticker: rel.target_ticker})
                ON CREATE SET target.name = coalesce(rel.target_company, rel.target_ticker)
                
                // With APOC we could create dynamic relationship types easily, 
                // but since we are in AuraDB Free, we use CALL apoc.create.relationship if available,
                // or standard Cypher with predefined match if APOC is not guaranteed.
                // A simpler, standard approach in Cypher without APOC for dynamic rels is slightly verbose,
                // but here is a safe way storing the type as a property if we don't use dynamic rel types,
                // OR we can make a generic RELATED_TO relationship and set the type there.
                // Alternatively, we group by relationship_type and run separate queries.
                """
                pass  # We will use the python loop approach below for simplicity with dynamic relationship types

            # Neo4j parameter execution approach for dynamic relationship types
            valid_rels_count = 0
            for rel in tqdm(rels_data, desc="Inserting relationships"):
                source_ticker = rel.get("source_ticker")
                target_ticker = rel.get("target_ticker")

                # Sanitize relationship type (Neo4j rel types cannot have spaces, usually uppercase)
                raw_type = rel.get("relationship_type", "RELATED_TO").upper()
                # Replace spaces and common special chars with underscore
                rel_type = (
                    raw_type.replace(" ", "_").replace("-", "_").replace("/", "_")
                )
                if not rel_type:
                    rel_type = "RELATED_TO"

                confidence = float(rel.get("confidence", 0.5))

                if not source_ticker or not target_ticker:
                    continue

                query = f"""
                MERGE (source:Company {{ticker: $source_ticker}})
                ON CREATE SET source.name = $source_name
                
                MERGE (target:Company {{ticker: $target_ticker}})
                ON CREATE SET target.name = $target_name
                
                MERGE (source)-[r:`{rel_type}`]->(target)
                SET r.confidence = $confidence,
                    r.original_type = $original_type
                """

                session.run(
                    query,
                    source_ticker=source_ticker,
                    source_name=rel.get("source_company", source_ticker),
                    target_ticker=target_ticker,
                    target_name=rel.get("target_company", target_ticker),
                    confidence=confidence,
                    original_type=rel.get("relationship_type", "related"),
                )
                valid_rels_count += 1

            logger.info(f"Successfully created {valid_rels_count} relationships.")

    except Exception as e:
        logger.error(f"Error during migration execution: {e}")
    finally:
        driver.close()
        logger.info("Neo4j driver closed.")
        logger.info("Migration script finished!")


if __name__ == "__main__":
    print(
        "Warning: This will read from Supabase and write to the configured Neo4j Aura instance."
    )
    print("Make sure your .env file is correctly configured.")
    response = input("Proceed with migration? (y/n): ")
    if response.lower() == "y":
        migrate_to_neo4j()
    else:
        print("Migration cancelled.")

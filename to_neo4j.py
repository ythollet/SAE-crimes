import pandas as pd
from sqlalchemy import create_engine
from neo4j import GraphDatabase
from tqdm import tqdm


# ==========================================
# 1. CONNEXIONS AUX DEUX BASES DE DONNÉES
# ==========================================

# Connexion à PostgreSQL (Source)
engine = create_engine('postgresql://postgres:postgres@localhost:5432/criminalite_db')

# Connexion à Neo4j (Cible)
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "01234567") 
driver = GraphDatabase.driver(URI, auth=AUTH)

# =========================
# 2. EXTRACTION DEPUIS SQL 
# =========================
print("📥 Extraction des données depuis PostgreSQL...")

# Extraction des futurs Noeuds
dict_departements = pd.read_sql("SELECT * FROM tbl_departements", engine).to_dict('records')
dict_communes = pd.read_sql("SELECT * FROM tbl_communes", engine).to_dict('records')
dict_perimetres = pd.read_sql("SELECT * FROM tbl_perimetres", engine).to_dict('records')
dict_postes = pd.read_sql("SELECT * FROM tbl_postes", engine).to_dict('records')
dict_infractions = pd.read_sql("SELECT * FROM tbl_infractions", engine).to_dict('records')

# Pour la table centrale, on fait la jointure pour récupérer l'année réelle au lieu de son ID
requete_faits = """
    SELECT ac.id_poste, ac.id_infraction, a.annee, ac.Nombre 
    FROM tbl_a_constate ac
    JOIN tbl_annees a ON ac.id_annee = a.id_annee
"""
dict_faits = pd.read_sql(requete_faits, engine).to_dict('records')

# Extraction des futures Relations (Les clés étrangères)
dict_rel_commune_dep = pd.read_sql("SELECT id_commune, id_departement FROM tbl_communes WHERE id_departement IS NOT NULL", engine).to_dict('records')
dict_rel_poste_commune = pd.read_sql("SELECT id_poste, id_commune FROM tbl_postes WHERE id_commune IS NOT NULL", engine).to_dict('records')
dict_rel_poste_perimetre = pd.read_sql("SELECT id_poste, id_perimetre FROM tbl_postes WHERE id_perimetre IS NOT NULL", engine).to_dict('records')
dict_rel_communes_voisines = pd.read_sql("SELECT * FROM tbl_communes_voisines WHERE id_commune_voisine IS NOT NULL", engine).to_dict('records')


print("✅ Extraction terminée !")



# ==========================================
# 3. INSERTION / MAJ DANS NEO4J (MERGE)
# ==========================================
print("📤 Insertion des données dans Neo4j...")

with driver.session(database="crimes") as session:
    
    # --- Création des index pour la performance ---
    session.run("CREATE INDEX poste_id IF NOT EXISTS FOR (p:Poste) ON (p.id);")
    session.run("CREATE INDEX commune_id IF NOT EXISTS FOR (c:Commune) ON (c.id);")
    session.run("CREATE INDEX dep_id IF NOT EXISTS FOR (d:Departement) ON (d.id);")
    session.run("CREATE INDEX perimetre_id IF NOT EXISTS FOR (pe:Perimetre) ON (pe.id);")
    session.run("CREATE INDEX infraction_id IF NOT EXISTS FOR (i:Infraction) ON (i.id);")
    session.run("CALL db.awaitIndexes();")

    # --- Insertion des Noeuds ---
    session.run("UNWIND $parametres AS row MERGE (d:Departement {id: toInteger(row.id_departement)}) SET d.libelle = row.departement", parametres=dict_departements)
    session.run("UNWIND $parametres AS row MERGE (c:Commune {id: toInteger(row.id_commune)}) SET c.libelle = row.commune", parametres=dict_communes)
    session.run("UNWIND $parametres AS row MERGE (pe:Perimetre {id: toInteger(row.id_perimetre)}) SET pe.libelle = row.perimetre", parametres=dict_perimetres)
    session.run("UNWIND $parametres AS row MERGE (i:Infraction {id: toInteger(row.id_infraction)}) SET i.libelle = row.infraction", parametres=dict_infractions)
    session.run("UNWIND $parametres AS row MERGE (p:Poste {id: toInteger(row.id_poste)}) SET p.libelle = row.poste, p.type = row.type_poste", parametres=dict_postes)

    # --- Insertion des Relations Structurelles ---
    session.run("""
        UNWIND $parametres AS row
        MATCH (c:Commune {id: toInteger(row.id_commune)})
        MATCH (d:Departement {id: toInteger(row.id_departement)})
        MERGE (c)-[:APPARTIENT_A]->(d)
    """, parametres=dict_rel_commune_dep)

    session.run("""
        UNWIND $parametres AS row
        MATCH (p:Poste {id: toInteger(row.id_poste)})
        MATCH (c:Commune {id: toInteger(row.id_commune)})
        MERGE (p)-[:EST_SITUE_DANS]->(c)
    """, parametres=dict_rel_poste_commune)

    session.run("""
        UNWIND $parametres AS row
        MATCH (p:Poste {id: toInteger(row.id_poste)})
        MATCH (pe:Perimetre {id: toInteger(row.id_perimetre)})
        MERGE (p)-[:EST_RATTACHE_A]->(pe)
    """, parametres=dict_rel_poste_perimetre)
    
    session.run("""
        UNWIND $parametres AS row
        MATCH (c1:Commune {id: toInteger(row.id_commune)})
        MATCH (c2:Commune {id: toInteger(row.id_commune_voisine)})
        MERGE (c1)-[:EST_VOISIN_DE]->(c2)
    """, parametres=dict_rel_communes_voisines)
    

    # --- Insertion des Faits (A_CONSTATE) en lots ---
    batch_size = 30000
    for idx in tqdm(range(0, len(dict_faits), batch_size), desc="A_CONSTATE"):
        session.run("""
            UNWIND $parametres AS row
            MATCH (p:Poste {id: toInteger(row.id_poste)})
            MATCH (i:Infraction {id: toInteger(row.id_infraction)})
            MERGE (p)-[r:A_CONSTATE {annee: toInteger(row.annee)}]->(i)
            SET r.nombre = toInteger(row.nombre)
        """, parametres=dict_faits[idx : idx + batch_size])


print("\n ✅ Migration SQL -> Neo4j terminée avec succès !")

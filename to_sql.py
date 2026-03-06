import pandas as pd
import re
import unicodedata
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text


def clean_col_commune(in_col):

    return (
        in_col
        .astype(str)                  # 1. Conversion en texte
        .str.strip()                  # 2. Nettoyage initial
        .str.upper()                  # 3. Mise en majuscules
        
        # --- Gestion des accents ---
        .str.normalize('NFD')         # 4. Décomposition (ex: 'é' devient 'e' + '´')
        .str.encode('ascii', errors='ignore') # 5. Suppression des caractères non-ASCII (les accents)
        .str.decode('utf-8')          # 6. Reconversion en texte standard
        
        # --- Nettoyage des caractères ---
        .str.replace(r'[^A-Z0-9\s]', ' ', regex=True) # 7. Suppression ponctuation
        .str.replace(r'\s+', ' ', regex=True)          # 8. Unification des espaces
        .str.strip()                  # 9. Nettoyage final
    )

def clean_nom_commune(in_nom):

    name = str(in_nom).strip().upper()

    # Supprimer les accents (Normalisation NFD)
    name = unicodedata.normalize('NFD', name).encode('ascii', 'ignore').decode('utf-8')
    
    # Remplacer TOUTE la ponctuation (tirets, apostrophes, etc.) par un espace
    name = re.sub(r'[^A-Z0-9\s]', ' ', name)
    
    # Remplacer les espaces multiples par un seul espace
    name = re.sub(r'\s+', ' ', name)
    
    return name

def extraire_commune(poste):
    name = str(poste).strip().upper()
    
    # 1. Enlever les numéros de département à la fin (2 ou 3 chiffres)
    name = re.sub(r'\s+\d{2,3}$', '', name)
    
    # 2. Liste des préfixes (Corrigée et générique)
    prefixes = [
        r"^CIAT\s+(CENTRAL|SUB)?\s*(DU|DES|DE\s+|D')?\s*",
        r"^CIAT\s*",
        r"^(CSP|CISP|CS)\s*(DU|DES|DE\s+|D')?\s*",
        r"^(DDSP|DSP|DTSP\d*)\s+(DU|DES|DE\s+|D')?\s*",
        r"^SURETE\s+(DEPARTEMENTALE|URBAINE)\s+(DU|DES|DE\s+|D')?\s*",
        r"^(DIPJ|DRPJ|SRPJ|DTPJ|SDPJ|STPJ|ANTENNE\s+PJ|ANTENNE\s+OFAST|OCRTIS)\s+(DU|DES|DE\s+|D')?\s*",
        r"^(SPAFA|SPAFP|SPAF|DDPAF|DIDPAF|DZPAF|DPAF|STPAF)\s+(BMR|BPA|CRA|UJI)?\s*(DU|DES|DE\s+|D')?\s*",
        r"^(BMRZ|BMRA|BMRT|BMR|BCFZ|BCFA|BCF|USG|UJI|UTE|CRA|GIR|CGD)\s+(DU|DES|DE\s+|D')?\s*",
        r"^(?:DUMZ\s+|UMZ\s+)?CRS\s+(?:\d+\s+|AUTO\s+)?(?:DETACHEMENT\s+|DET\s+)?(?:DE\s+|D')?\s*"
    ]
    
    for prefix in prefixes:
        name = re.sub(prefix, '', name)

    # Nettoyage de la ponctuation et des espaces
    name = clean_nom_commune(name)

    if name == "BMRA":
        return ''
    
    return name.strip()



def process_communes_voisines():

    df_communes_voisines = pd.read_csv("communes_adjacentes_2022.csv",usecols=[1,4], sep=';').rename(columns={"nom": "commune", "noms_voisins":"communes_voisines"})
    df_communes_voisines['communes_voisines'] = df_communes_voisines['communes_voisines'].str.split('|')
    df_communes_voisines = df_communes_voisines.explode('communes_voisines')

    df_communes_voisines['commune'] = clean_col_commune(df_communes_voisines['commune'])
    df_communes_voisines['communes_voisines'] = clean_col_commune(df_communes_voisines['communes_voisines'])
    df_communes_voisines = df_communes_voisines.rename(columns={'communes_voisines': 'commune_voisine'})
    df_communes_voisines = df_communes_voisines[df_communes_voisines['commune']!=df_communes_voisines['commune_voisine']]
    return df_communes_voisines

df_communes_voisines = process_communes_voisines()


def process_GN(in_excel_file, in_sheet_name):
    # Déduction du type de poste et de l'année depuis le nom de la feuille
    type_poste = 'GN' 
    annee = int(in_sheet_name.split()[-1])

    # Lecture brute sans header pour bien capter les 2 premières lignes
    df_raw = pd.read_excel(in_excel_file, sheet_name=in_sheet_name, header=None)
    
    # Extraction des listes de départements (ligne 0) et postes (ligne 1) à partir de la 3ème colonne
    depts = df_raw.iloc[0, 2:].astype(str).to_dict()
    postes = df_raw.iloc[1, 2:].astype(str).to_dict()
    communes = {k_poste : extraire_commune(v_poste) for k_poste, v_poste in postes.items()}

    
    # Isoler les données réelles (à partir de la 3ème ligne)
    df_data = df_raw.iloc[2:,1:].copy()
    df_data.rename(columns={1: 'infraction'}, inplace=True)
    
    # Transformation des colonnes en lignes
    df_melted = df_data.melt(
        id_vars=['infraction'], 
        var_name='col_index', 
        value_name='nombre'
    )
    
   
    # Ajout des colonnes de dimensions
    df_melted['departement'] = df_melted['col_index'].map(depts)
    df_melted['commune'] =  df_melted['col_index'].map(communes)
    df_melted['poste'] = df_melted['col_index'].map(postes)
    df_melted['type_poste'] = type_poste
    df_melted['annee'] = annee
    
    # Nettoyage des données
    df_melted = df_melted.dropna(subset=['nombre'])
    df_melted['nombre'] = pd.to_numeric(df_melted['nombre'], errors='coerce').fillna(0).astype(int)
    
    # --- AJOUT CRUCIAL : Ne garder que les crimes ayant vraiment eu lieu ---
    df_melted = df_melted[df_melted['nombre'] > 0]
    
    # On supprime la colonne technique
    df_melted.drop(columns=['col_index'], inplace=True)
    
    return df_melted



def process_PN(in_excel_file, in_sheet_name):
    
    # Déduction du type de poste et de l'année depuis le nom de la feuille
    type_poste = 'PN' 
    annee = int(in_sheet_name.split()[-1])

    # Lecture brute sans header pour bien capter les 2 premières lignes
    df_raw = pd.read_excel(in_excel_file, sheet_name=in_sheet_name, header=None)

    # Extraction des listes de départements (ligne 0) et postes (ligne 1) à partir de la 3ème colonne
    depts = df_raw.iloc[0, 2:].astype(str).to_dict()
    perimetres = df_raw.iloc[1, 2:].astype(str).to_dict()
    postes = df_raw.iloc[2, 2:].astype(str).to_dict()

    communes = {k_poste : extraire_commune(v_poste) for k_poste, v_poste in postes.items()}
    
    # Isoler les données réelles (à partir de la 3ème ligne)
    df_data = df_raw.iloc[3:,1:].copy()

    df_data.rename(columns={1: 'infraction'}, inplace=True)

    # Transformation des colonnes en lignes
    df_melted = df_data.melt(
        id_vars=['infraction'], 
        var_name='col_index', 
        value_name='nombre'
    )
    
    # Ajout des colonnes de dimensions
    df_melted['departement'] = df_melted['col_index'].map(depts)
    df_melted['poste'] = df_melted['col_index'].map(postes)
    df_melted['commune'] =  df_melted['col_index'].map(communes)
    df_melted['perimetre'] = df_melted['col_index'].map(perimetres)
    df_melted['type_poste'] = type_poste
    df_melted['annee'] = annee
    
    # Nettoyage des données
    df_melted = df_melted.dropna(subset=['nombre'])
    df_melted['nombre'] = pd.to_numeric(df_melted['nombre'], errors='coerce').fillna(0).astype(int)
    
    # On supprime la colonne technique
    df_melted.drop(columns=['col_index'], inplace=True)
    
    return df_melted


def process_sheet(in_excel_file, in_sheet_name):

    if 'PN' in in_sheet_name:
        df = process_PN(
            in_excel_file = in_excel_file,
            in_sheet_name = in_sheet_name
        )

    
    elif 'GN' in in_sheet_name:
        df = process_GN(
            in_excel_file = in_excel_file,
            in_sheet_name = in_sheet_name
        )
    
    else:
        df = pd.DataFrame()

    return df


def get_max_id(engine, table_name, id_column):
    """Récupère l'ID maximum d'une table donnée. Retourne 0 si la table n'existe pas."""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT MAX({id_column}) FROM {table_name}")
            result = conn.execute(query)
            max_id = result.scalar()
            return max_id if max_id is not None else 0
    except Exception:
        # La table n'existe probablement pas encore
        return 0



# --- 1. LECTURE DE TOUTES LES FEUILLES ---
file_path = "data.xlsx"
in_excel_file = pd.ExcelFile(file_path)
list_df = [process_sheet(in_excel_file, nom_onglet) for nom_onglet in in_excel_file.sheet_names]

# Concaténation de tout l'historique dans un "Master" dataframe
df_master = pd.concat(list_df, ignore_index=True)

engine = create_engine('postgresql://postgres:postgres@localhost:5432/criminalite_db')


# --- 2. CRÉATION DES TABLES DU MCD ---

def sync_dimension(engine, table_name, df_source, subset_cols, id_col):
    """
    Vérifie la base SQL, isole les nouvelles lignes, génère les nouveaux ID,
    insère dans PostgreSQL, et retourne le mapping complet.
    """
    try:
        df_sql = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    except Exception:
        df_sql = pd.DataFrame(columns=[id_col] + subset_cols)

    df_source_unique = df_source[subset_cols].drop_duplicates()
    merged = df_source_unique.merge(df_sql, on=subset_cols, how='left', indicator=True)
    df_new = merged[merged['_merge'] == 'left_only'][subset_cols].copy()

    if not df_new.empty:
        max_id = df_sql[id_col].max() if not df_sql.empty else 0
        if pd.isna(max_id): max_id = 0
        
        df_new[id_col] = range(int(max_id) + 1, int(max_id) + 1 + len(df_new))
        
        df_new.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"➕ {len(df_new)} nouvelles entrées ajoutées à {table_name}.")
        
        df_sql = pd.concat([df_sql, df_new], ignore_index=True)
    else:
        print(f"ℹ️ Aucune nouvelle donnée pour {table_name}.")

    return df_sql


# ========================================
# --- FONCTION DE CRÉATION DE LA BASE ---
# =======================================
def create_sql_database():
    try:
        conn = psycopg2.connect(user="postgres", password="postgres", host="localhost", port="5432", dbname="postgres")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute("ALTER DATABASE template1 REFRESH COLLATION VERSION;")
        cursor.execute("ALTER DATABASE postgres REFRESH COLLATION VERSION;")
        
        cursor.execute("CREATE DATABASE criminalite_db;")
        print("✅ Base de données 'criminalite_db' créée avec succès !")
    except psycopg2.errors.DuplicateDatabase:
        print("ℹ️ La base de données 'criminalite_db' existe déjà.")
    except Exception as e:
        print(f"Erreur lors de la création de la base : {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()


# ===================================
# --- FONCTION DE SYNCHRONISATION ---
# ===================================
def run_migration():
    print("\n🔄 Début de la synchronisation avec PostgreSQL...")
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/criminalite_db')

    tbl_departements = sync_dimension(engine, 'tbl_departements', df_master[['departement']], ['departement'], 'id_departement')
    tbl_perimetres = sync_dimension(engine, 'tbl_perimetres', df_master[['perimetre']], ['perimetre'], 'id_perimetre')
    tbl_annees = sync_dimension(engine, 'tbl_annees', df_master[['annee']], ['annee'], 'id_annee')
    tbl_infractions = sync_dimension(engine, 'tbl_infractions', df_master[['infraction']], ['infraction'], 'id_infraction')

    df_communes_prep = df_master[['commune', 'departement']].drop_duplicates().merge(tbl_departements, on='departement')
    tbl_communes = sync_dimension(engine, 'tbl_communes', df_communes_prep, ['commune', 'id_departement'], 'id_commune')

    df_postes_prep = (
        df_master[['poste', 'type_poste', 'commune', 'perimetre']]
        .drop_duplicates()
        .merge(tbl_communes[['commune', 'id_commune']], on='commune')
        .merge(tbl_perimetres[['perimetre', 'id_perimetre']], on='perimetre')
    )
    tbl_postes = sync_dimension(engine, 'tbl_postes', df_postes_prep, ['poste', 'type_poste', 'id_commune', 'id_perimetre'], 'id_poste')

    # 3. Table de liaison (Communes Voisines)
    # On utilise directement df_communes_voisines au lieu de df_master 
    df_cv_prep = (
        df_communes_voisines[['commune','commune_voisine']].dropna().drop_duplicates()
        .merge(tbl_communes[['commune', 'id_commune']], on='commune')
        .rename(columns={'id_commune': 'id_commune_source'})
        .merge(tbl_communes[['commune', 'id_commune']], left_on='commune_voisine', right_on='commune')
        .rename(columns={'id_commune': 'id_commune_voisine'})
    )[['id_commune_source', 'id_commune_voisine']].rename(columns={'id_commune_source':'id_commune'})

    try:
        df_cv_sql = pd.read_sql("SELECT * FROM tbl_communes_voisines", engine)
        merged_cv = df_cv_prep.merge(df_cv_sql, on=['id_commune', 'id_commune_voisine'], how='left', indicator=True)
        df_cv_new = merged_cv[merged_cv['_merge'] == 'left_only'][['id_commune', 'id_commune_voisine']]
    except Exception:
        df_cv_new = df_cv_prep

    if not df_cv_new.empty:
        df_cv_new.to_sql('tbl_communes_voisines', engine, if_exists='append', index=False)
        print(f"➕ {len(df_cv_new)} nouvelles relations de voisinage ajoutées.")
    else:
        print("ℹ️ Aucune nouvelle relation de voisinage.")

    # 4. Table A_Constate
    print("\n📊 Préparation de la table des A_Constate...")
    tbl_postes_complet = df_postes_prep.merge(tbl_postes[['poste', 'type_poste', 'id_commune', 'id_perimetre', 'id_poste']], on=['poste', 'type_poste', 'id_commune', 'id_perimetre'])

    tbl_a_constate = (
        df_master[['poste', 'type_poste', 'commune', 'perimetre', 'infraction', 'annee', 'nombre']]
        .merge(tbl_postes_complet[['id_poste', 'poste', 'type_poste', 'commune', 'perimetre']], on=['poste', 'type_poste', 'commune', 'perimetre'])
        .merge(tbl_infractions[['id_infraction','infraction']], on='infraction')
        .merge(tbl_annees[['id_annee','annee']], on='annee')
    )
    tbl_a_constate = tbl_a_constate[['id_poste', 'id_infraction', 'id_annee', 'nombre']]
    tbl_a_constate = tbl_a_constate.groupby(['id_poste', 'id_infraction', 'id_annee'], as_index=False)['nombre'].sum()

    try:
        df_ac_sql = pd.read_sql("SELECT id_poste, id_infraction, id_annee FROM tbl_a_constate", engine)
        merged_ac = tbl_a_constate.merge(df_ac_sql, on=['id_poste', 'id_infraction', 'id_annee'], how='left', indicator=True)
        tbl_a_constate_new = merged_ac[merged_ac['_merge'] == 'left_only'].drop(columns=['_merge'])
    except Exception:
        tbl_a_constate_new = tbl_a_constate

    if not tbl_a_constate_new.empty:
        tbl_a_constate_new.to_sql('tbl_a_constate', engine, if_exists='append', index=False)
        print(f"{len(tbl_a_constate_new)} nouveaux constats d'infractions ajoutés.")
    else:
        print("ℹ️ Aucun nouveau constat d'infraction. La base est déjà à jour.")

    # 5. Ajout de la clé primaire pour la table A_constate
    from sqlalchemy import exc, text
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE tbl_a_constate ADD PRIMARY KEY (id_poste, id_infraction, id_annee);"))
            conn.commit()
            print("Clé primaire ajoutée avec succès à tbl_a_constate.")
        except Exception as e:
            pass


    print("\n✅ Migration SQL terminée et sécurisée contre les doublons !")


if __name__ == "__main__":
    create_sql_database() # Crée la base si elle n'existe pas
    run_migration()       # Lance la synchronisation Pandas -> SQL
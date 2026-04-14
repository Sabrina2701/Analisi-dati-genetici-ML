import psycopg2

# Connessione al primo database (origine)
vecchia_db_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '1947934',
    'host': 'localhost',
    'port': '5432'
}

# Connessione al secondo database (destinazione)
nuova_db_config = {
    'dbname': 'nuovo_db',
    'user': 'postgres',
    'password': '1947934',
    'host': 'localhost',
    'port': '5432'
}

"""
def measurement_type(vecchia_db_config, nuova_db_config):
    ""
    Questa funzione si connette al vecchio database per ottenere i nomi dei geni
    dalla tabella gene_expression_file e li inserisce nel nuovo database
    nella tabella measurement_type con le unità di misura associate.
    ""
    # Nomi delle unità di misura
    units = ['tpm', 'fpkm', 'fpkm_uq', 'unstranded', 'stranded_first', 'stranded_second']

    try:
        # Connessione al vecchio database
        old_conn = psycopg2.connect(**vecchia_db_config)
        old_cur = old_conn.cursor()
        
        # Ottenere i nomi dei geni dalla tabella gene_expression_file
        old_cur.execute("SELECT DISTINCT gene FROM gene_expression_file")
        gene_names = old_cur.fetchall()
        
        # Connessione al nuovo database
        new_conn = psycopg2.connect(**nuova_db_config)
        new_cur = new_conn.cursor()
        
        # Inserire i nomi dei geni e le unità di misura nella tabella measurement_type
        insert_query = ""
        INSERT INTO measurement_type (name, unita_misura) VALUES (%s, %s) RETURNING measure_id
        ""
        gene_to_measure_id = {}
        genes_added = set()  # Set per tenere traccia dei geni già inseriti
        for gene_row in gene_names:
            gene_name = gene_row[0]  # Accedi al primo elemento della tupla, che è il gene_name

            if gene_name not in genes_added:
                for unit in units:
                    new_cur.execute(insert_query, (gene_name, unit))
                    measure_id = new_cur.fetchone()[0]
                    gene_to_measure_id[(gene_name, unit)] = measure_id
                genes_added.add(gene_name)  # Aggiungi il gene al set una volta che tutte le unità di misura sono state aggiunte

        # Inserire 'vital_status' con unita_misura vuoto
        new_cur.execute(insert_query, ('vital_status', ''))
        measure_id = new_cur.fetchone()[0]
        gene_to_measure_id[('vital_status', '')] = measure_id
        
        # Commit delle operazioni nel nuovo database
        new_conn.commit()
        
        return gene_to_measure_id

    except Exception as e:
        print(f"Errore: {e}")
        
    finally:
        # Chiudere le connessioni e i cursori
        if old_cur:
            old_cur.close()
        if old_conn:
            old_conn.close()
        if new_cur:
            new_cur.close()
        if new_conn:
            new_conn.close()


def populate_measurements(vecchia_db_config, nuova_db_config, gene_to_measure_id):
    ""
    Popola la tabella measurements con i dati delle misurazioni dal vecchio database.
    Aggiunge anche i valori di vital_status.
    ""
    # Ottenere la mappatura tra analysis e case_id
    analysis_to_case = get_analysis_to_case_mapping(vecchia_db_config)

    # Query per ottenere i valori delle misurazioni dal vecchio database
    select_query = ""
    SELECT analysis, gene, tpm, fpkm, fpkm_uq, unstranded, stranded_first, stranded_second
    FROM gene_expression_file
    ORDER BY analysis  -- Assicuriamoci che i risultati siano ordinati per analysis
    ""
    
    # Query per ottenere case_id e vital_status
    case_query = ""
    SELECT case_id, vital_status FROM "case"
    ""
    
    # Query per inserire i valori delle misurazioni nel nuovo database
    insert_query = ""
    INSERT INTO measurements (measure_id, sample_id, measure_value)
    VALUES (%s, %s, %s)
    ""
    
    old_conn = None
    old_cur = None
    new_conn = None
    new_cur = None

    try:
        # Connessione al vecchio database
        old_conn = psycopg2.connect(**vecchia_db_config)
        old_cur = old_conn.cursor()
        
        # Ottenere i valori delle misurazioni
        old_cur.execute(select_query)
        measurements = old_cur.fetchall()

        # Ottenere i dati di case_id e vital_status
        old_cur.execute(case_query)
        case_data = old_cur.fetchall()
        case_dict = {cid: vs for cid, vs in case_data}
        
        # Connessione al nuovo database
        new_conn = psycopg2.connect(**nuova_db_config)
        new_cur = new_conn.cursor()
        
        # Inizializzazione delle variabili
        current_sample_id = 1
        previous_analysis = measurements[0][0] if measurements else None
        
        # Inserire i valori delle misurazioni nel nuovo database
        processed_analyses = set()
        for measurement in measurements:
            analysis = measurement[0]
            gene_name = measurement[1]
            case_id = analysis_to_case.get(analysis)
            
            # Controlla se analysis è cambiato rispetto alla riga precedente
            if analysis != previous_analysis:
                if case_id in case_dict:
                    vital_status = case_dict[case_id]
                    measure_id = gene_to_measure_id.get(('vital_status', ''))
                    if measure_id is not None:
                        new_cur.execute(insert_query, (measure_id, current_sample_id, vital_status))
                current_sample_id += 1
                processed_analyses.add(analysis)
            
            # Inserisci i valori delle misurazioni dei geni
            for i, unit in enumerate(['tpm', 'fpkm', 'fpkm_uq', 'unstranded', 'stranded_first', 'stranded_second']):
                value = measurement[i+2]  # Inizia da 2 perché il primo elemento è analysis, il secondo è gene_name
                measure_id = gene_to_measure_id.get((gene_name, unit))
                if measure_id is not None:
                    new_cur.execute(insert_query, (measure_id, current_sample_id, value))
            
            # Aggiorna previous_analysis
            previous_analysis = analysis
        
        # Gestisci le analysis rimanenti che non hanno misurazioni dei geni
        for analysis, case_id in analysis_to_case.items():
            if analysis not in processed_analyses and case_id in case_dict:
                vital_status = case_dict[case_id]
                measure_id = gene_to_measure_id.get(('vital_status', ''))
                if measure_id is not None:
                    new_cur.execute(insert_query, (measure_id, current_sample_id, vital_status))
                current_sample_id += 1
        
        # Commit delle operazioni nel nuovo database
        new_conn.commit()

    except Exception as e:
        print(f"Errore: {e}")
        
    finally:
        # Chiudere le connessioni e i cursori
        if old_cur:
            old_cur.close()
        if old_conn:
            old_conn.close()
        if new_cur:
            new_cur.close()
        if new_conn:
            new_conn.close()

def get_analysis_to_case_mapping(vecchia_db_config):
    ""
    Ottiene la mappatura tra analysis e case_id dal vecchio database.
    ""
    mapping_query = ""
    SELECT a.analysis, b.case
    FROM analysis_entity a
    JOIN biospecimen b ON a.biospecimen_id = b.id
    ""
    
    conn = None
    cur = None

    try:
        conn = psycopg2.connect(**vecchia_db_config)
        cur = conn.cursor()
        cur.execute(mapping_query)
        mapping = cur.fetchall()
        return {analysis: case_id for analysis, case_id in mapping}

    except Exception as e:
        print(f"Errore: {e}")
        return {}

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

"""

def inserire_analysis(vecchia_db_config, nuova_db_config):
    """
    Copia i dati della colonna 'analysis' dalla tabella 'gene_expression_file'
    alla colonna 'analysis_id' della tabella 'sample_type' del nuovo database.
    """


    try:
        old_conn = psycopg2.connect(**vecchia_db_config)
        old_cursor = old_conn.cursor()
        
        new_conn = psycopg2.connect(**nuova_db_config)
        new_cursor = new_conn.cursor()

        # Recupero dei dati dalla tabella 'gene_expression_file'
        old_cursor.execute("SELECT DISTINCT analysis FROM gene_expression_file")
        analysis_data = old_cursor.fetchall()
        
        # Inserimento dei dati nella tabella 'sample_type' del nuovo database
        insert_query = "INSERT INTO sample_type (analysis_data) VALUES (%s)"
        for row in analysis_data:
            new_cursor.execute(insert_query, (row[0],))
        
        # Commit delle operazioni sul nuovo database
        new_conn.commit()
        
        print("Dati copiati con successo dal vecchio database al nuovo database.")
    
    except Exception as e:
        print(f"Errore durante la copia dei dati: {e}")
    
    finally:
        # Chiusura delle connessioni
        if old_cursor:
            old_cursor.close()
        if old_conn:
            old_conn.close()
        if new_cursor:
            new_cursor.close()
        if new_conn:
            new_conn.close()


#Chiamo funzione per ripredere id analysis
inserire_analysis(vecchia_db_config, nuova_db_config)

# Chiamata alla funzione per migrare i dati dei geni
#gene_to_measure_id = measurement_type(vecchia_db_config, nuova_db_config)

# Chiamata alla funzione per inserire i valori delle misurazioni
#populate_measurements(vecchia_db_config, nuova_db_config, gene_to_measure_id)
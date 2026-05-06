# Notes de développement

## DAG Airflow — évolutions récentes

### dbt snapshot (`dbt/snapshots/snp_netflix_titles.sql`)
- Ajout d'un snapshot dbt sur `stg_netflix_titles`
- Stratégie `check` sur les colonnes métier (`title`, `type`, `country`, `release_year`, `listed_in`)
- Clé unique : `show_id`
- Capte les modifications ligne par ligne à chaque run (SCD type 2)

### Tâche `dbt_snapshot` dans le DAG
- Nouvelle tâche `BashOperator` insérée entre `dbt_seed` et `dbt_run`
- Chaîne dbt complète : `dbt_seed >> dbt_snapshot >> dbt_run >> summary`

### Dépendances explicites (`set_downstream`)
- Remplacement du chaînage `>>` par des appels `set_downstream()` individuels
- La bifurcation depuis `clean_data` et la convergence vers `summary` sont maintenant lisibles directement

### Horodatage des fichiers de sortie (`execution_date`)
- `task_clean_data` : génère `outputs/cleaned_data_YYYY-MM-DD.csv` via `context.get("ds")`
- `task_generate_report` : génère `outputs/report_YYYY-MM-DD.json` (et `.html`)
- Le chemin daté est transmis aux tâches en aval via XCom (`cleaned_output_path`, `report_path`)
- Fallback sur la date UTC courante pour les runs manuels sans `logical_date` (Airflow 3.x)

### Montage direct du dossier `dags/`
- `docker-compose.yaml` modifié : le volume `dags` pointe désormais directement sur
  `/Users/registesson/dev/data-pipeline-csv-sqlite/dags/`
- Plus besoin de copie manuelle ni de symlink après chaque modification du DAG

## Gestion d'erreur et retries du DAG

### `on_failure_callback`
- Ajout de `on_task_failure` enregistré dans `DEFAULT_ARGS` → actif sur toutes les tâches
- Loggue : `DAG`, `task_id`, `run_id`, numéro d'essai et exception à chaque échec

### Stratégie de retry différenciée par tâche
- `insert_to_db` : `retries=3`, `retry_exponential_backoff=True` (délai de base 2 min) — absorbe les contentions SQLite
- `load_csv` / `copy_to_dbt_seed` : retry rapide (30s) — problèmes I/O transitoires
- `dbt_seed/snapshot/run` : délai long (3 min) — dbt initialise son environnement au démarrage
- `summary` : `retries=0`, `TriggerRule.ALL_DONE` — s'exécute même si une branche en amont échoue

### `execution_timeout` par tâche
- Toutes les tâches ont un timeout explicite pour éviter les blocages silencieux
- Plages : 2 min (`summary`) → 15 min (`dbt_*`)

### Validation dans les callables
- Vérification d'existence du fichier source avant `load_csv`
- Vérification du XCom (`cleaned_output_path`) avant chaque tâche qui en dépend
- `clean_data` bloque si le DataFrame nettoyé est vide (évite une insertion creuse)
- Exceptions wrappées avec contexte (`raise ... from exc`) pour traçabilité dans les logs

## Tests unitaires du DAG

### `tests/test_dag_tasks.py` (19 tests)
- Teste les callables directement avec un contexte Airflow mocké (`MagicMock` pour `ti`)
- Couvre : `FileNotFoundError` (fichier manquant, XCom None/inexistant), `ValueError`
  (CSV vide, colonnes manquantes, résultat vide), `RuntimeError` (erreurs DB, rendu, copie OSError)
- Vérifie les XCom pushes sur les happy paths
- Aucune infrastructure Airflow requise : chemins surchargés via `monkeypatch`

### `tests/test_dag_structure.py` (28 tests)
- Inspecte l'objet `dag` directement à l'import — aucun run Airflow
- Vérifie : présence des 9 tâches, graphe de dépendances complet (parametrize), retry/timeout
  par tâche (parametrize), `retry_exponential_backoff` sur `insert_to_db`, `TriggerRule.ALL_DONE`
  sur `summary`, `on_failure_callback` présent et correct sur toutes les tâches
- Utile comme filet de sécurité lors d'une refacto du DAG

### `conftest.py` (racine)
- Ajoute `dags/` au `sys.path` de pytest
- Permet d'importer `netflix_pipeline_dag` sans `__init__.py` dans `dags/`

## Points d'attention

- **Airflow 3.x** : `context["ds"]` lève `KeyError` pour les runs manuels sans `logical_date`.
  Utiliser `context.get("ds") or datetime.now(timezone.utc).strftime("%Y-%m-%d")`.
- **Imports dépréciés** : `airflow.operators.bash.BashOperator` et `airflow.operators.python.PythonOperator`
  sont dépréciés en Airflow 3.x — migrer vers `airflow.providers.standard.operators.*`.
- **Volume dags** : défini via la variable `NETFLIX_DAGS_DIR` dans `docker-compose.yaml`
  (valeur par défaut : chemin absolu du repo).

#!/usr/bin/env zsh
# =============================================================================
# setup_airflow.sh — Installation et configuration d'Apache Airflow
#                    pour le projet data-pipeline-csv-sqlite
# =============================================================================
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AIRFLOW_HOME="${PROJECT_ROOT}/.airflow"
AIRFLOW_VERSION="2.9.3"
PYTHON_VERSION="$(python3 --version | cut -d ' ' -f2 | cut -d '.' -f1-2)"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Setup Apache Airflow ${AIRFLOW_VERSION}"
echo "  Python : ${PYTHON_VERSION}"
echo "  AIRFLOW_HOME : ${AIRFLOW_HOME}"
echo "  DAGs : ${PROJECT_ROOT}/dags"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── 1. Installation d'Airflow avec contraintes ─────────────────────────────────
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "\n[1/4] Installation d'Apache Airflow..."
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# ── 2. Configuration de AIRFLOW_HOME ──────────────────────────────────────────
export AIRFLOW_HOME="${AIRFLOW_HOME}"
mkdir -p "${AIRFLOW_HOME}"

echo "\n[2/4] Initialisation de la base de données Airflow..."
airflow db migrate

# ── 3. Configuration du dossier dags ──────────────────────────────────────────
echo "\n[3/4] Configuration du dossier dags → ${PROJECT_ROOT}/dags"

# Patch airflow.cfg pour pointer vers le dossier dags du projet
AIRFLOW_CFG="${AIRFLOW_HOME}/airflow.cfg"
if [ -f "${AIRFLOW_CFG}" ]; then
    sed -i '' "s|^dags_folder = .*|dags_folder = ${PROJECT_ROOT}/dags|" "${AIRFLOW_CFG}"
    echo "  ✅ dags_folder mis à jour dans airflow.cfg"
fi

# ── 4. Création de l'utilisateur admin ────────────────────────────────────────
echo "\n[4/4] Création de l'utilisateur admin Airflow..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname Netflix \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅ Setup terminé !"
echo ""
echo "  Pour démarrer Airflow, lancez dans deux terminaux séparés :"
echo ""
echo "    export AIRFLOW_HOME=${AIRFLOW_HOME}"
echo "    airflow webserver --port 8080"
echo ""
echo "    export AIRFLOW_HOME=${AIRFLOW_HOME}"
echo "    airflow scheduler"
echo ""
echo "  Interface web : http://localhost:8080"
echo "  Login : admin / admin"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"


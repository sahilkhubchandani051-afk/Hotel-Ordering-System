#!/bin/bash
set -e

# Run unified migration script
echo "Applying database migrations..."
python -u migrate_production.py

# Start Gunicorn
echo "Starting application with Gunicorn..."
exec gunicorn app.main:app --workers 2 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:${PORT:-8000} --timeout 120 --log-level info --access-logfile - --error-logfile -

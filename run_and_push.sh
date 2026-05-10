#!/bin/bash
# Run full pipeline and push results to GitHub when done

cd /Users/mhoque/Documents/stock-fraud-screener

echo "Starting full pipeline at $(date)"
python3 run.py --full

if [ $? -eq 0 ]; then
    echo "Pipeline done. Pushing to GitHub..."
    git add data/companies_financials.json data/fraud_signals.json data/companies_filtered.json reports/fraud_report.json 2>/dev/null
    git commit -m "Update fraud report with full dataset $(date '+%Y-%m-%d')"
    git push
    echo "Done! Streamlit will auto-update shortly."
else
    echo "Pipeline failed. Check /tmp/pipeline_auto.log for errors."
fi

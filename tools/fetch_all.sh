#!/bin/bash
RUNID="$1"

if [ -z "$RUNID" ]; then
  echo "Usage: $0 RUNID"
  exit 1
fi

scp -r "root@8.218.96.252:/root/bn_research_core/output/state/*${RUNID}.*" \
  ~/ai_projects/bn_research_core/state

scp -r "root@8.218.96.252:/root/bn_research_core/output/visual_audit/${RUNID}" \
  ~/ai_projects/bn_research_core/visual_audit/${RUNID}
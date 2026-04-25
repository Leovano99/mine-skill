#!/bin/bash

DIR="$(cd "$(dirname "$0")" && pwd)"

PYTHON="$DIR/.venv/bin/python"
SCRIPT="$DIR/scripts/run_tool.py"

$PYTHON $SCRIPT --session instance1 agent-start ds_wikipedia &
$PYTHON $SCRIPT --session instance2 agent-start ds_wikipedia &
$PYTHON $SCRIPT --session instance3 agent-start ds_wikipedia &

wait

echo "All agents started.
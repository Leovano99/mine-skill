
#!/bin/bash

DIR="$(cd "$(dirname "$0")" && pwd)"

PYTHON="$DIR/.venv/bin/python"
SCRIPT="$DIR/scripts/run_tool.py"

$PYTHON $SCRIPT --session instance1 agent-control stop &
$PYTHON $SCRIPT --session instance2 agent-control stop &
$PYTHON $SCRIPT --session instance3 agent-control stop &

# wait for all background jobs to finish
wait

echo "All instances stopped."

# cleanup
rm -rf "$DIR/output"

echo "Output folder removed."
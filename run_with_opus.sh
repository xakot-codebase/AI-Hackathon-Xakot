#!/bin/bash
# Run Xakot application with Opus API configuration

export OPUS_BASE_URL="https://operator.opus.com"
export OPUS_WORKFLOW_ID="f3fa002a-23f6-456a-8b26-6dfa7463992e"
export OPUS_SERVICE_KEY="_72e20c74d55e077caadad65dbb1a1ac6c5bb5a406a602112a311b30d35c58872e01fff88609e55ab6d69317366637962"

echo "Starting Xakot application with Opus API..."
echo "Base URL: $OPUS_BASE_URL"
echo "Workflow ID: $OPUS_WORKFLOW_ID"
echo ""

cd "$(dirname "$0")"
source .venv/bin/activate
python app.py


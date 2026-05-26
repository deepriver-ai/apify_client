#!/usr/bin/env sh

if [ -n "${BASH_SOURCE:-}" ]; then
  SCRIPT_PATH="${BASH_SOURCE}"
elif [ -n "${ZSH_VERSION:-}" ]; then
  SCRIPT_PATH="${(%):-%x}"
else
  SCRIPT_PATH="$0"
fi

APIFY_CLIENT_DIR="$(CDPATH= cd -- "$(dirname -- "$SCRIPT_PATH")" && pwd)"
ELASTIC_CLIENT_DIR="/Users/oscarcuellar/ocn/media/elastic_client"

case ":${PYTHONPATH:-}:" in
  *":$APIFY_CLIENT_DIR:"*) ;;
  *) PYTHONPATH="$APIFY_CLIENT_DIR${PYTHONPATH:+:$PYTHONPATH}" ;;
esac

case ":$PYTHONPATH:" in
  *":$ELASTIC_CLIENT_DIR:"*) ;;
  *) PYTHONPATH="$ELASTIC_CLIENT_DIR:$PYTHONPATH" ;;
esac

export PYTHONPATH
echo "PYTHONPATH=$PYTHONPATH"

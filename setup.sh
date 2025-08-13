#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$PROJECT_ROOT/venv"  # keep name consistent with original script
PYTHON_BIN="python3"
ENTRYPOINT_NAME="decomp-pd-ingest"  # as defined in pyproject [project.scripts]

FORCE=0
QUIET=0

usage() {
	cat <<EOF
Usage: ./setup.sh [options]

Options:
	-f, --force     Recreate virtual environment even if it exists.
	-q, --quiet     Reduced output.
	-h, --help      Show this help.
EOF
}

log() { [ "$QUIET" -eq 1 ] || echo -e "$*"; }

while [[ $# -gt 0 ]]; do
	case "$1" in
		-f|--force) FORCE=1; shift ;;
		-q|--quiet) QUIET=1; shift ;;
		-h|--help) usage; exit 0 ;;
		*) echo "Unknown option: $1" >&2; usage; exit 1 ;;
	esac
done

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
	echo "Error: $PYTHON_BIN not found in PATH." >&2
	exit 1
fi

# Ensure Python version >= 3.10
PY_VERSION=$($PYTHON_BIN -c 'import sys; print("%d.%d"%sys.version_info[:2])')
MIN_MAJOR=3
MIN_MINOR=10
CUR_MAJOR=${PY_VERSION%%.*}
CUR_MINOR=${PY_VERSION#*.}
if [[ $CUR_MAJOR -lt $MIN_MAJOR || ( $CUR_MAJOR -eq $MIN_MAJOR && $CUR_MINOR -lt $MIN_MINOR ) ]]; then
	echo "Error: Python >= $MIN_MAJOR.$MIN_MINOR required (found $PY_VERSION)." >&2
	exit 1
fi

if [[ -d "$VENV_DIR" && $FORCE -eq 1 ]]; then
	log "Removing existing virtual environment (force requested)..."
	rm -rf "$VENV_DIR"
fi

if [[ ! -d "$VENV_DIR" ]]; then
	log "Creating virtual environment in $VENV_DIR"
	"$PYTHON_BIN" -m venv "$VENV_DIR"
else
	log "Using existing virtual environment at $VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

log "Upgrading pip/setuptools/wheel..."
python -m pip install --upgrade pip setuptools wheel >/dev/null

log "Installing project (editable wheel)..."
pip install .

# Copiar .env.example se .env nao existir
if [[ ! -f "$PROJECT_ROOT/.env" && -f "$PROJECT_ROOT/.env.example" ]]; then
	cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"
	log "Arquivo .env criado a partir de .env.example (ajuste os valores conforme necessário)."
fi

# Determine a suitable directory on PATH for symlink
choose_link_dir() {
	# Preference order: ~/.local/bin, any writable dir in PATH.
	local candidate
	if [[ -d "$HOME/.local/bin" || ! -e "$HOME/.local/bin" ]]; then
		mkdir -p "$HOME/.local/bin"
		echo "$HOME/.local/bin"
		return 0
	fi
	IFS=":" read -r -a path_parts <<< "$PATH"
	for candidate in "${path_parts[@]}"; do
		if [[ -n "$candidate" && -d "$candidate" && -w "$candidate" ]]; then
			echo "$candidate"
			return 0
		fi
	done
	return 1
}

LINK_DIR="$(choose_link_dir || true)"

if [[ -z "${LINK_DIR:-}" ]]; then
	echo "Warning: No writable directory on PATH found for symlink. Skipping symlink creation." >&2
	echo "You can manually link: ln -s '$VENV_DIR/bin/$ENTRYPOINT_NAME' /some/dir/on/PATH/$ENTRYPOINT_NAME" >&2
else
	ln -sf "$VENV_DIR/bin/$ENTRYPOINT_NAME" "$LINK_DIR/$ENTRYPOINT_NAME"
	log "Created symlink: $LINK_DIR/$ENTRYPOINT_NAME -> $VENV_DIR/bin/$ENTRYPOINT_NAME"
	case ":$PATH:" in
		*":$LINK_DIR:"*) : ;; # already on PATH
		*) echo "Note: $LINK_DIR is not currently on PATH. Add the following line to your shell profile:"; echo "  export PATH=\"$LINK_DIR:\$PATH\"" ;; 
	esac
fi

log "Setup complete. You can now run: $ENTRYPOINT_NAME"

deactivate >/dev/null 2>&1 || true
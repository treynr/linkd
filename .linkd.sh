
## file: .linkd.rc
## desc: Configuration file for the linkd pipeline.

log() { printf "[%s] %s\n" "$(date '+%Y.%m.%d %H:%M:%S')" "$*"; }

BASE_DIR="."
## Base data directory
DATA_DIR="$BASE_DIR/data"
## Source directory with scripts
SRC_DIR="$BASE_DIR/src"
## File from the 1K Genome Project that maps samples to populations
SAMPLE_MAP="./data"

DIRS=($DATA_DIR)

for d in "${DIRS[@]}"
do
    [[ ! -f "$d" ]] && mkdir -p "$d";
done


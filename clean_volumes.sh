# Define the directories to clean
directories=("./data/db" "./data/elasticsearch" "./data/vocab-fetch" "./data/files")

# Define the paths to exclude
exclude_paths=(
    "./data/db/virtuoso.ini"
    "./data/vocab-fetch/.gitkeep"
)

# Function to delete files and directories while excluding specified paths
clean_directory() {
    local dir=$1
    shift
    local excludes=("$@")

    # Build the find command with exclusion patterns
    find_cmd="find \"$dir\" -mindepth 1"
    for exclude in "${excludes[@]}"; do
        find_cmd+=" ! -path \"$exclude\""
    done

    # Execute the find command and delete the matched files/directories
    echo "Executing: $find_cmd -exec rm -rf {} +"
    eval "$find_cmd -exec rm -rf {} +"
}

# Clean each directory
for dir in "${directories[@]}"; do
    if [[ ! -d "$dir" ]]; then
        echo "Error: Directory $dir does not exist"
        continue
    fi
    clean_directory "$dir" "${exclude_paths[@]}"
done
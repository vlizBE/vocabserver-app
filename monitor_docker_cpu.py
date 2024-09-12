import os
import subprocess
import time
import argparse

# Function to get the container's CPU usage
def get_cpu_usage(container_id):
    try:
        # Get the container's CPU usage using docker stats with no-stream option
        stats = subprocess.check_output(["docker", "stats", container_id, "--no-stream", "--format", "{{.CPUPerc}}"])
        # Convert the CPU percentage from the output
        cpu_usage = float(stats.decode('utf-8').strip('%\n'))
        return cpu_usage
    except Exception as e:
        print(f"Error getting CPU usage for container {container_id}: {e}")
        return None

# Function to restart the container
def restart_container(container_id):
    try:
        # Restart the container using docker-compose (or docker if needed)
        os.system(f"docker restart {container_id}")
        print(f"Container '{container_id}' restarted due to high CPU usage.")
    except Exception as e:
        print(f"Error restarting container {container_id}: {e}")

# Main function that checks and restarts the container based on CPU usage
def monitor_container(container_id, cpu_threshold, check_interval):
    while True:
        cpu_usage = get_cpu_usage(container_id)
        if cpu_usage:
            print(f"Current CPU usage of {container_id}: {cpu_usage}%")
            if cpu_usage > cpu_threshold:
                restart_container(container_id)
        else:
            print(f"Failed to get CPU usage for {container_id}")
        
        # Sleep for the specified interval before checking again
        time.sleep(check_interval)

# Entry point of the script
if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Monitor Docker container CPU usage and restart if threshold is exceeded.")
    parser.add_argument("container_id", type=str, help="The ID or name of the Docker container to monitor.")
    parser.add_argument("--cpu-threshold", type=float, default=300.0, help="CPU usage threshold in percentage (default is 300%)")
    parser.add_argument("--check-interval", type=int, default=60, help="Interval in seconds to check CPU usage (default is 60 seconds)")
    
    args = parser.parse_args()

    # Start monitoring the container with the given arguments
    monitor_container(args.container_id, args.cpu_threshold, args.check_interval)

# Stop all running containers
docker ps -aq | ForEach-Object { docker stop $_ }

# Remove all containers
docker ps -aq | ForEach-Object { docker rm $_ }

# Remove all images
docker images -q | ForEach-Object { docker rmi $_ }

# Remove all volumes
docker volume ls -q | ForEach-Object { docker volume rm $_ }

# Navigate to your Docker Compose project directory
Set-Location -Path "."

# Take down the Docker Compose services and remove volumes
docker-compose down -v

# Optional: If you want to also remove the Docker networks created by Docker Compose
docker network prune -f

# Build containers
docker-compose .\build

# Start containers
docker-compose up

# For some reason the second time always works
docker-compose up
